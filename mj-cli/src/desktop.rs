//! `mj app`: the daemon's web viewer in a native desktop window.
//!
//! The daemon serves the viewer over loopback HTTP (or Tailscale HTTPS). The
//! desktop shell pins exactly one TLS certificate and installs a
//! pre-authorized session cookie, so this module puts a TLS-terminating
//! loopback proxy in front of the daemon's viewer: an ephemeral self-signed
//! certificate satisfies the shell's pinning, and a cookie minted from the
//! daemon's own persisted signing key satisfies authentication — the shell
//! runs as the same user as the daemon, so possession of the key is the
//! credential. The proxy forwards every request, including the viewer's
//! server-sent events stream, and dies with the window.

use std::net::SocketAddr;

use anyhow::{Context, Result, bail};
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Request, Response, StatusCode, header};
use hel::hel_server::{
    COOKIE_NAME, cookie_key_path, load_or_create_cookie_key, mint_desktop_session_cookie,
};

use crate::daemon::{self, WebViewerStatus};

/// Headers that must not be forwarded in either direction: they describe the
/// connection between two specific peers, not the request.
const HOP_BY_HOP: [header::HeaderName; 7] = [
    header::CONNECTION,
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

#[derive(Clone)]
struct ProxyState {
    client: reqwest::Client,
    upstream: String,
}

pub(crate) async fn run_desktop_app() -> Result<()> {
    let mut client = daemon::connect_or_start().await?;
    let status = client.status().await?;
    let viewer_url = match status.phone_status {
        WebViewerStatus::Ready { viewer_url, .. } => viewer_url,
        WebViewerStatus::Disabled => {
            bail!(
                "the web viewer is disabled; enable [phone] in config.toml and run `mj daemon restart`"
            )
        }
        WebViewerStatus::Starting => {
            bail!("the web viewer is still starting; retry in a moment or check `mj daemon status`")
        }
        WebViewerStatus::Stopped => {
            bail!("the web viewer is stopped; run `mj daemon restart`")
        }
        WebViewerStatus::Error { message } => {
            bail!("the web viewer failed to start: {message}; run `mj daemon restart`")
        }
    };

    let key = load_or_create_cookie_key(&cookie_key_path())
        .context("read the viewer cookie signing key")?;
    let cookie_value =
        mint_desktop_session_cookie(&key).context("mint a desktop viewer session")?;

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .context("generate the desktop TLS certificate")?;
    let certificate_der = certified.cert.der().to_vec();
    let key_der = certified.key_pair.serialize_der();
    let tls =
        axum_server::tls_rustls::RustlsConfig::from_der(vec![certificate_der.clone()], key_der)
            .await
            .context("build the desktop TLS configuration")?;

    let proxy_state = ProxyState {
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("build the desktop proxy client")?,
        upstream: viewer_url.trim_end_matches('/').to_owned(),
    };
    let router = Router::new().fallback(proxy).with_state(proxy_state);

    let handle = axum_server::Handle::new();
    let server =
        axum_server::bind_rustls("127.0.0.1:0".parse::<SocketAddr>()?, tls).handle(handle.clone());
    let server_task = tokio::spawn(server.serve(router.into_make_service()));

    let bound = handle
        .listening()
        .await
        .context("bind the desktop proxy listener")?;
    let origin: url::Url = format!("https://localhost:{}/", bound.port())
        .parse()
        .context("build the desktop viewer origin")?;

    println!("Opening the Mjolnir desktop viewer at {origin}");
    let (shell_tx, shell_rx) = tokio::sync::oneshot::channel::<mj_desktop::DesktopShellRemote>();
    let watchdog = tokio::spawn(async move {
        // If the proxy dies while the window is open, surface that in the
        // shell instead of leaving a window quietly showing stale content. A
        // normal shutdown aborts this task first, and failing an
        // already-closed shell is a no-op.
        let outcome = server_task.await;
        if let Ok(shell) = shell_rx.await {
            shell.fail(match outcome {
                Ok(Ok(())) => "the desktop proxy exited unexpectedly".to_owned(),
                Ok(Err(error)) => format!("the desktop proxy failed: {error}"),
                Err(error) => format!("the desktop proxy panicked: {error}"),
            });
        }
    });

    let shell_result = mj_desktop::run(
        mj_desktop::DesktopShellOptions {
            origin,
            certificate_der,
            bootstrap_cookie_name: COOKIE_NAME,
            bootstrap_cookie_value: cookie_value,
        },
        move |shell| {
            let _ = shell_tx.send(shell);
        },
    );

    handle.shutdown();
    watchdog.abort();
    shell_result.map(|_| ())
}

async fn proxy(State(state): State<ProxyState>, request: Request<Body>) -> Response<Body> {
    match forward(state, request).await {
        Ok(response) => response,
        Err(error) => {
            let mut response = Response::new(Body::from(format!(
                "the desktop proxy could not reach the Mjolnir daemon: {error:#}"
            )));
            *response.status_mut() = StatusCode::BAD_GATEWAY;
            response
        }
    }
}

async fn forward(state: ProxyState, request: Request<Body>) -> Result<Response<Body>> {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |part| part.as_str());
    let url = format!("{}{path_and_query}", state.upstream);
    let (parts, body) = request.into_parts();

    let mut upstream = state
        .client
        .request(parts.method, url)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()));
    for (name, value) in filtered(&parts.headers) {
        upstream = upstream.header(name.clone(), value.clone());
    }
    let response = upstream.send().await.context("forward to the daemon")?;

    let mut builder = Response::builder().status(response.status());
    for (name, value) in filtered(response.headers()) {
        builder = builder.header(name.clone(), value.clone());
    }
    builder
        .body(Body::from_stream(response.bytes_stream()))
        .context("assemble the proxied response")
}

fn filtered(
    headers: &HeaderMap,
) -> impl Iterator<Item = (&header::HeaderName, &header::HeaderValue)> {
    headers
        .iter()
        .filter(|(name, _)| *name != header::HOST && !HOP_BY_HOP.contains(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hop_by_hop_and_host_headers_are_not_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "localhost:1".parse().unwrap());
        headers.insert(header::CONNECTION, "keep-alive".parse().unwrap());
        headers.insert(header::COOKIE, "hel_viewer_session=x".parse().unwrap());
        headers.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        let kept: Vec<_> = filtered(&headers).map(|(name, _)| name.clone()).collect();
        assert_eq!(kept.len(), 2, "{kept:?}");
        assert!(kept.contains(&header::COOKIE));
        assert!(kept.contains(&header::ACCEPT));
    }
}

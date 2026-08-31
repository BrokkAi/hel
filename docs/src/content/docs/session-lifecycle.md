---
title: Session lifecycle
description: Know when to leave, finish, resume, hide, or delete a Hel session.
---

Hel keeps navigation separate from lifecycle. Returning to the dashboard does
not stop work, and quitting Hel does not finish a session.

## The ordinary workflow

While a session is active:

- Press **Ctrl+G**, or use `/dashboard`, to return to the dashboard. The worker
  and any queued prompts keep running.
- Press **Ctrl+F** on the dashboard when you are done with the session. Hel
  calls this **Finish**.

Finish waits for the work that is already running, prevents new queued work
from starting, saves queued prompts for an optional later resume, and writes
and verifies recovery. Only then does Hel release the session's target.
The confirmation says exactly which resource will be released before you
approve it.

Quitting the terminal dashboard is not Finish. If live sessions remain, Hel
counts them and asks you to confirm **Quit, keep running**.

## What Finish releases

The effect follows the target's ownership boundary; there is no extra cleanup
policy to choose for every session.

- Local Podman and Apple container targets remove the session container. The
  computer and other containers remain.
- Podman over SSH removes the session container. The SSH host and its other
  containers remain.
- A Hel-created EC2 target terminates that session's EC2 instance.
- Local bare targets stop the Hel worker and remove Hel runtime files. The
  selected project directory remains.
- Bare SSH targets stop the remote Hel worker and remove its Hel runtime
  files. The SSH host and selected project directory remain.

If recovery cannot be verified, normal Finish does not release the target.
Hel offers Retry and an explicit **Force finish** path; force finishing may
lose work newer than the last verified recovery copy.

## Saved sessions

A successfully finished session moves to **Saved sessions**. Saved sessions
run no workers and incur no live container, host, or instance compute cost.
They retain a recovery archive on the controller, so they still use local
disk. The Saved screen shows the available per-session sizes and their total.

You do not need to decide a retention deadline when finishing. Saved sessions
remain recoverable until you delete them.

- **Resume** restores the logical session onto a fresh target and asks whether
  its saved prompt queue should start or be discarded.
- **Hide** removes a session from the default Saved list. It does not stop a
  worker, remove recovery, or free disk.
- **Delete permanently** removes the verified recovery archive and logical
  session record. It cannot be resumed afterward.

In short: use Dashboard when you are merely stepping away, Finish when this
run is complete, Resume when you want another run, Hide for organization, and
Delete permanently only when you no longer need recovery.

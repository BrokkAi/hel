import { renderMarkdown } from './markdown.js';

/// Build one element. Every piece of this application creates nodes and sets
/// `textContent`; nothing builds markup as a string, which is what makes agent
/// output structurally unable to inject an element.
function el(name, className, textContent) {
  const node = document.createElement(name);
  if (className) node.className = className;
  if (textContent !== undefined) node.textContent = textContent;
  return node;
}

/// A button carrying the data a click handler reads back off it.
function button(label, className, data) {
  const node = el('button', className, label);
  for (const [key, value] of Object.entries(data || {})) node.dataset[key] = value;
  return node;
}

const login = document.querySelector('#login'),
  app = document.querySelector('#app'),
  dashboard = document.querySelector('#dashboard'),
  conversation = document.querySelector('#conversation'),
  sessions = document.querySelector('#sessions'),
  configured = document.querySelector('#configured'),
  logout = document.querySelector('#logout'),
  newForm = document.querySelector('#new-form'),
  newProfile = document.querySelector('#new-profile'),
  newBundle = document.querySelector('#new-bundle'),
  newTarget = document.querySelector('#new-target'),
  newProjectDirectory = document.querySelector('#new-project-directory'),
  actionError = document.querySelector('#action-error'),
  feed = document.querySelector('#conversation-feed'),
  queue = document.querySelector('#conversation-queue'),
  shells = document.querySelector('#conversation-shells'),
  elicitations = document.querySelector('#elicitations'),
  promptText = document.querySelector('#prompt-text'),
  attachments = document.querySelector('#attachments'),
  attachImage = document.querySelector('#attach-image'),
  imagePicker = document.querySelector('#image-picker');
/// Transcript nodes by entry id, so an update patches the row it belongs to
/// rather than searching the whole document for it.
const entryNodes = new Map();
let snapshot,
  currentSession,
  cursor = 0,
  acknowledged = 0,
  eventSource;
async function request(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: { 'content-type': 'application/json', ...(options.headers || {}) },
  });
  if (response.status === 401) throw new Error('unauthorized');
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || response.statusText);
  }
  if (response.status === 202 || response.status === 204) return null;
  return response.json();
}
function fillOptions(select, items, selected) {
  select.replaceChildren(
    ...items.map(item => {
      const option = el('option', '', item.id);
      option.value = item.id;
      if (item.id === selected) option.selected = true;
      return option;
    }),
  );
}

function syncProjectDirectory() {
  const required =
    snapshot?.targets.find(x => x.id === newTarget.value)?.requires_project_directory === true;
  newProjectDirectory.classList.toggle('hidden', !required);
  newProjectDirectory.required = required;
  if (!required) newProjectDirectory.value = '';
}
function startEvents() {
  if (eventSource) eventSource.close();
  eventSource = new EventSource('/api/events');
  eventSource.addEventListener('revision', () => {
    refresh();
    if (currentSession) loadConversation(true);
  });
}
/// One session row.
///
/// Which actions appear is still read from the session's state string; the
/// plan replaces that with the daemon's own `ViewerSessionCapabilities` in
/// Milestone 3, and this is the only place that will have to change.
function sessionCard(session) {
  const card = el('article', 'card session');
  card.append(el('h3', '', session.title));

  const status = el('p');
  status.append(el('span', 'pill', session.state));
  if (session.has_error) status.append(el('span', 'pill alert', 'needs attention'));
  if (session.pending_elicitations?.length) {
    status.append(el('span', 'pill alert', 'input needed'));
  }
  status.append(el('span', '', ` ${session.harness_kind} \u00b7 ${session.profile_id}`));
  card.append(status);

  const queued = (session.queued_prompts || []).length;
  card.append(
    el('p', 'dim', `${session.bundle_id} \u2192 ${session.target_id} \u00b7 ${queued} queued`),
  );

  if (session.preview?.length) {
    card.append(el('p', 'preview', session.preview.join('\n')));
  }

  const actions = el('div', 'row');
  const open = button('Open', '', { action: 'open', id: session.id });
  open.disabled = !session.conversation_available;
  actions.append(open);
  if (session.state === 'provisioning') {
    actions.append(button('Cancel', 'danger', { action: 'cancel', id: session.id }));
  } else {
    actions.append(
      button('Resume', '', {
        action: 'resume',
        id: session.id,
        profile: session.profile_id,
        target: session.target_id,
      }),
      button('Stop', 'danger', { action: 'close', id: session.id }),
    );
  }
  card.append(actions);
  return card;
}

function profileRow(profile) {
  const row = el('p');
  row.append(el('strong', '', profile.id), el('span', '', ` \u00b7 ${profile.harness_kind}`));
  row.append(el('br'));
  const quota = profile.quota
    ? profile.quota.summary +
      (profile.quota.stale ? ' \u00b7 stale' : '') +
      (profile.quota.has_error ? ' \u00b7 unavailable' : '')
    : 'quota unavailable';
  row.append(el('span', 'dim', quota));
  return row;
}

async function refresh() {
  try {
    snapshot = await request('/api/snapshot');
    login.classList.add('hidden');
    app.classList.remove('hidden');
    logout.classList.remove('hidden');
    if (!newProfile.value) fillOptions(newProfile, snapshot.profiles);
    if (!newBundle.value) fillOptions(newBundle, snapshot.bundles);
    if (!newTarget.value) fillOptions(newTarget, snapshot.targets);
    syncProjectDirectory();
    sessions.replaceChildren(...snapshot.sessions.map(sessionCard));
    if (!snapshot.sessions.length) {
      sessions.append(el('p', 'dim', 'No Hel-managed sessions.'));
    }
    configured.replaceChildren(
      ...snapshot.profiles.map(profileRow),
      el(
        'p',
        'dim',
        `${snapshot.targets.length} targets \u00b7 ${snapshot.bundles.length} bundles`,
      ),
    );
    if (currentSession) {
      const session = snapshot.sessions.find(x => x.id === currentSession);
      if (!session?.conversation_available) {
        showDashboard();
      } else {
        renderQueue(session);
        renderElicitations(session);
        renderAttachments();
        document.querySelector('#conversation-state').textContent = session.state;
      }
    }
    if (!eventSource) startEvents();
    return true;
  } catch (e) {
    if (e.message === 'unauthorized') {
      snapshot = undefined;
      currentSession = null;
      if (eventSource) {
        eventSource.close();
        eventSource = undefined;
      }
      login.classList.remove('hidden');
      app.classList.add('hidden');
      logout.classList.add('hidden');
    }
    return false;
  }
}
async function restoreRoute() {
  if (!(await refresh())) return;
  const match = location.hash.match(/^#conversation\/([A-Za-z0-9_-]+)$/);
  if (match) await openConversation(match[1]);
}
function renderQueue(session) {
  const prompts = session.queued_prompts || [];
  queue.replaceChildren(
    ...(prompts.length
      ? prompts.map((prompt, index) => {
          const row = el('div', 'queue-item');
          row.append(el('span', '', `${index + 1}. ${prompt.text}`));
          row.append(button('Remove', 'danger', { queueId: prompt.id }));
          return row;
        })
      : [el('p', 'dim', 'No queued prompts.')]),
  );
  const running = session.active_user_shells || [];
  shells.replaceChildren(
    ...(running.length
      ? running.map(shell => {
          const row = el('div', 'queue-item');
          row.append(el('span', '', `$ ${shell.command}`));
          row.append(button('Cancel', 'danger', { shellId: shell.id }));
          return row;
        })
      : [el('p', 'dim', 'No running shells.')]),
  );
}
// Every snapshot revision re-renders the conversation. Rebuilding a card the
// user is answering would wipe the half-filled form and steal focus, so each
// pending request keeps its live DOM until the request itself changes or
// leaves the snapshot.
const elicitationCards = new Map(),
  sentElicitations = new Set();
function elicitationKey(sessionId, id) {
  return `${sessionId}\u001f${id}`;
}
function elicitationOptionLabel(option) {
  return option.description ? `${option.title} \u2014 ${option.description}` : option.title;
}
function elicitationControl(field) {
  if (field.kind === 'single_select' || field.kind === 'multi_select') {
    const select = document.createElement('select');
    select.multiple = field.kind === 'multi_select';
    if (!select.multiple && !field.required) select.appendChild(new Option('', ''));
    for (const option of field.options || [])
      select.appendChild(new Option(elicitationOptionLabel(option), option.value));
    if (field.kind === 'single_select' && field.default != null) select.value = field.default;
    if (select.multiple && (field.default || []).length)
      for (const option of select.options) option.selected = field.default.includes(option.value);
    return select;
  }
  const input = document.createElement('input');
  input.type =
    field.kind === 'boolean'
      ? 'checkbox'
      : field.kind === 'integer' || field.kind === 'number'
        ? 'number'
        : field.secret
          ? 'password'
          : 'text';
  if (field.kind === 'integer') input.step = '1';
  if (field.kind === 'number') input.step = 'any';
  if (field.minimum != null) input.min = field.minimum;
  if (field.maximum != null) input.max = field.maximum;
  if (field.min_length != null) input.minLength = field.min_length;
  if (field.max_length != null) input.maxLength = field.max_length;
  if (field.pattern) input.pattern = field.pattern;
  if (field.kind === 'boolean') input.checked = field.default === true;
  else if (field.default != null) input.value = String(field.default);
  return input;
}
function elicitationFieldValue(field, control) {
  if (field.kind === 'multi_select') {
    const values = [...control.selectedOptions].map(option => option.value);
    return values.length || field.required ? values : undefined;
  }
  if (field.kind === 'boolean') return control.checked;
  if (control.value === '')
    return field.required && (field.kind === 'text' || field.kind === 'single_select')
      ? ''
      : undefined;
  if (field.kind === 'integer') return Number.parseInt(control.value, 10);
  if (field.kind === 'number') return Number(control.value);
  return control.value;
}
// Builds the controls and returns collect(), which reads them back as ACP
// content. A custom answer replaces the select it belongs to unless the
// request pairs it with one specific option, which is how Hel's chat form
// submits the same request.
function buildElicitationForm(form, request, register) {
  const entries = [];
  for (const field of request.fields || []) {
    const wrapper = document.createElement('label');
    wrapper.className = 'elicitation-field';
    const label = document.createElement('span');
    label.textContent = `${field.title}${field.required ? ' *' : ''}`;
    const control = elicitationControl(field);
    control.required = Boolean(field.required) && field.kind !== 'boolean';
    register(control);
    wrapper.append(label, control);
    if (field.description) {
      const description = document.createElement('span');
      description.className = 'dim';
      description.textContent = field.description;
      wrapper.append(description);
    }
    if (field.kind === 'multi_select') {
      const check = () => {
        const count = control.selectedOptions.length;
        const few =
          field.min_items != null && (count > 0 || field.required) && count < field.min_items;
        const many = field.max_items != null && count > field.max_items;
        control.setCustomValidity(
          few
            ? `Select at least ${field.min_items} option(s).`
            : many
              ? `Select at most ${field.max_items} option(s).`
              : '',
        );
      };
      control.addEventListener('change', check);
      check();
    }
    form.append(wrapper);
    entries.push({ field, control });
  }
  const customByOwner = new Map();
  for (const entry of entries) {
    const owner = entry.field.custom_answer_for;
    if (!owner || entry.field.kind !== 'text' || customByOwner.has(owner)) continue;
    const target = entries.find(candidate => candidate.field.id === owner);
    if (!target || !Array.isArray(target.field.options)) continue;
    customByOwner.set(owner, entry);
  }
  return () => {
    for (const entry of entries)
      if (entry.field.kind === 'text') entry.control.value = entry.control.value.trim();
    if (!form.reportValidity()) return null;
    const active = new Map();
    for (const [owner, entry] of customByOwner)
      if (entry.control.value !== '') active.set(owner, entry);
    const content = {};
    for (const entry of entries) {
      const { field, control } = entry;
      if (customByOwner.get(field.custom_answer_for) === entry) {
        if (active.has(field.custom_answer_for)) content[field.id] = control.value;
        continue;
      }
      const custom = active.get(field.id);
      if (custom && custom.field.custom_answer_option == null) continue;
      const value = elicitationFieldValue(field, control);
      if (value !== undefined) content[field.id] = value;
    }
    return content;
  };
}
function buildElicitationCard(session, request) {
  const card = document.createElement('section');
  card.className = 'card elicitation';
  const heading = document.createElement('strong');
  heading.textContent = request.title || 'Input needed';
  const message = document.createElement('pre');
  message.className = 'elicitation-message';
  message.textContent = request.message;
  const form = document.createElement('form');
  const status = document.createElement('p');
  status.className = 'dim';
  const gated = [],
    register = control => {
      gated.push(control);
      return control;
    };
  const collect = buildElicitationForm(form, request, register);
  const actions = document.createElement('div');
  actions.className = 'row';
  const send = document.createElement('button');
  send.type = 'submit';
  send.textContent = 'Send answer';
  register(send);
  const decline = document.createElement('button');
  decline.type = 'button';
  decline.className = 'secondary';
  decline.textContent = 'Decline';
  register(decline);
  const cancel = document.createElement('button');
  cancel.type = 'button';
  cancel.className = 'danger';
  cancel.textContent = 'Cancel';
  register(cancel);
  decline.addEventListener('click', () => {
    submitElicitation(session.id, request.id, { action: 'decline' });
  });
  cancel.addEventListener('click', () => {
    submitElicitation(session.id, request.id, { action: 'cancel' });
  });
  actions.append(send, decline, cancel);
  form.append(actions);
  form.addEventListener('submit', event => {
    event.preventDefault();
    const content = collect();
    if (content) submitElicitation(session.id, request.id, { action: 'accept', content });
  });
  const nodes = [heading];
  if (request.description) {
    const description = document.createElement('p');
    description.className = 'dim';
    description.textContent = request.description;
    nodes.push(description);
  }
  nodes.push(message, form, status);
  card.append(...nodes);
  return {
    card,
    setSent(sent) {
      for (const control of gated) control.disabled = sent;
      status.textContent = sent ? 'Answer sent \u2014 waiting for the session to apply it.' : '';
    },
  };
}
function renderElicitations(session) {
  const pending = (session && session.pending_elicitations) || [];
  if (session)
    for (const key of [...sentElicitations])
      if (
        key.startsWith(`${session.id}\u001f`) &&
        !pending.some(request => elicitationKey(session.id, request.id) === key)
      )
        sentElicitations.delete(key);
  const live = new Set(),
    cards = [];
  for (const request of pending) {
    const key = elicitationKey(session.id, request.id),
      signature = JSON.stringify(request);
    live.add(key);
    let entry = elicitationCards.get(key);
    if (!entry || entry.signature !== signature) {
      entry = buildElicitationCard(session, request);
      entry.signature = signature;
      elicitationCards.set(key, entry);
    }
    entry.setSent(sentElicitations.has(key));
    cards.push(entry.card);
  }
  for (const key of [...elicitationCards.keys()]) if (!live.has(key)) elicitationCards.delete(key);
  const mounted = [...elicitations.children];
  if (mounted.length !== cards.length || cards.some((card, index) => mounted[index] !== card))
    elicitations.replaceChildren(...cards);
}
async function submitElicitation(sessionId, elicitationId, response) {
  const key = elicitationKey(sessionId, elicitationId);
  if (sentElicitations.has(key)) return;
  sentElicitations.add(key);
  const rerender = () => {
    const session = snapshot?.sessions.find(x => x.id === sessionId);
    if (session && sessionId === currentSession) renderElicitations(session);
  };
  rerender();
  try {
    await request('/api/actions', {
      method: 'POST',
      body: JSON.stringify({
        action: 'respond-elicitation',
        session_id: sessionId,
        elicitation_id: elicitationId,
        response,
      }),
    });
    document.querySelector('#conversation-error').textContent = '';
    await refresh();
  } catch (err) {
    sentElicitations.delete(key);
    document.querySelector('#conversation-error').textContent = err.message;
    rerender();
  }
}
// The composer is a contenteditable rather than a textarea so a pasted or
// dropped image can be intercepted where it lands, and so the box grows with
// its content without a layout read on every keystroke. Rich content is
// refused at beforeinput, which keeps the box plain text however it arrives.
const MAX_PROMPT_REQUEST_BYTES = 32 * 1024 * 1024;
let composerRevision = 0,
  composerPreserveEmptyBreak = false,
  promptImages = [];
function composerText() {
  let text = '';
  const blocks = new Set(['DIV', 'P']);
  const append = node => {
    if (node.nodeType === Node.TEXT_NODE) {
      text += node.nodeValue || '';
      return;
    }
    if (node.nodeName === 'BR') {
      if (!node.dataset.composerFiller) text += '\n';
      return;
    }
    const block = node !== promptText && blocks.has(node.nodeName);
    if (block && text && !text.endsWith('\n')) text += '\n';
    node.childNodes.forEach(append);
    if (block && node.nextSibling && !text.endsWith('\n')) text += '\n';
  };
  append(promptText);
  return text.replace(/\r\n?/g, '\n');
}
function setComposerText(text) {
  promptText.textContent = text;
}
function placeComposerCaretAtEnd() {
  const selection = window.getSelection();
  if (!selection) return;
  const range = document.createRange();
  range.selectNodeContents(promptText);
  range.collapse(false);
  selection.removeAllRanges();
  selection.addRange(range);
}
function placeComposerCaretAtPoint(x, y) {
  let range = document.caretRangeFromPoint?.(x, y) || null;
  if (!range && document.caretPositionFromPoint) {
    const position = document.caretPositionFromPoint(x, y);
    if (position) {
      range = document.createRange();
      range.setStart(position.offsetNode, position.offset);
      range.collapse(true);
    }
  }
  if (!range || !promptText.contains(range.startContainer)) return;
  const selection = window.getSelection();
  if (!selection) return;
  selection.removeAllRanges();
  selection.addRange(range);
}
function insertComposerFallback(node, filler = null) {
  const selection = window.getSelection();
  const range = selection && selection.rangeCount ? selection.getRangeAt(0) : null;
  if (!range || !promptText.contains(range.commonAncestorContainer)) {
    promptText.append(node);
    if (filler) promptText.append(filler);
    placeComposerCaretAtEnd();
    return;
  }
  range.deleteContents();
  range.insertNode(node);
  if (filler) node.after(filler);
  range.setStartAfter(node);
  range.collapse(true);
  selection.removeAllRanges();
  selection.addRange(range);
}
// execCommand keeps the browser's own undo stack, so it is tried first; the
// fallback covers engines that refuse it, and the revision check covers those
// that run it without emitting the input event that keeps state in step.
function runComposerEdit(command, value, fallback) {
  promptText.focus();
  const revision = composerRevision;
  if (document.execCommand(command, false, value)) {
    if (composerRevision === revision) composerInputChanged();
    return;
  }
  fallback();
  composerInputChanged();
}
function insertComposerText(text) {
  const normalized = text.replace(/\r\n?/g, '\n');
  runComposerEdit('insertText', normalized, () => {
    insertComposerFallback(document.createTextNode(normalized));
  });
}
function insertComposerLineBreak() {
  composerPreserveEmptyBreak = true;
  try {
    runComposerEdit('insertLineBreak', null, () => {
      const filler = document.createElement('br');
      filler.dataset.composerFiller = 'true';
      insertComposerFallback(document.createElement('br'), filler);
    });
    let last = promptText;
    while (last.lastChild) last = last.lastChild;
    if (last.nodeName === 'BR' && last.previousSibling?.nodeName === 'BR') {
      last.dataset.composerFiller = 'true';
    }
  } finally {
    composerPreserveEmptyBreak = false;
  }
}
// A cleared box can keep a stray break behind it, which leaves the placeholder
// hidden and the box looking occupied when it holds nothing.
function composerInputChanged() {
  composerRevision += 1;
  if (!composerPreserveEmptyBreak && !promptText.textContent && promptText.childNodes.length)
    promptText.replaceChildren();
}
function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.addEventListener('load', () => resolve(String(reader.result || '')), { once: true });
    reader.addEventListener('error', () => reject(reader.error || new Error('file read failed')), {
      once: true,
    });
    reader.readAsDataURL(file);
  });
}
function imageDimensions(file) {
  return new Promise((resolve, reject) => {
    const url = URL.createObjectURL(file);
    const image = new Image();
    image.addEventListener(
      'load',
      () => {
        const size = { width: image.naturalWidth, height: image.naturalHeight };
        URL.revokeObjectURL(url);
        resolve(size);
      },
      { once: true },
    );
    image.addEventListener(
      'error',
      () => {
        URL.revokeObjectURL(url);
        reject(new Error('the browser could not decode this image'));
      },
      { once: true },
    );
    image.src = url;
  });
}
async function promptImageFromFile(file) {
  if (!file.type.startsWith('image/'))
    throw new Error(`${file.name || 'That file'} is not an image`);
  if (file.size >= MAX_PROMPT_REQUEST_BYTES)
    throw new Error(`${file.name || 'That image'} is too large for the 32 MiB request limit`);
  const [dataUrl, size] = await Promise.all([readFileAsDataUrl(file), imageDimensions(file)]);
  const comma = dataUrl.indexOf(',');
  if (comma < 0 || !dataUrl.slice(comma + 1))
    throw new Error(`Could not read ${file.name || 'that image'}`);
  return {
    data_base64: dataUrl.slice(comma + 1),
    mime_type: file.type,
    width: size.width,
    height: size.height,
    name: file.name || 'Pasted image',
  };
}
async function attachImageFiles(files) {
  const session = snapshot?.sessions.find(x => x.id === currentSession);
  if (!currentSession || !session?.prompt_images_supported || !files.length) return;
  const sessionId = currentSession;
  try {
    const added = [];
    for (const file of files) added.push(await promptImageFromFile(file));
    if (currentSession !== sessionId) return;
    promptImages = promptImages.concat(added);
    renderAttachments();
    document.querySelector('#conversation-error').textContent = '';
  } catch (err) {
    document.querySelector('#conversation-error').textContent = err.message;
  }
}
function renderAttachments() {
  const session = snapshot?.sessions.find(x => x.id === currentSession);
  attachImage.hidden = !session?.prompt_images_supported;
  attachments.replaceChildren();
  for (const [index, image] of promptImages.entries()) {
    const chip = document.createElement('div');
    chip.className = 'attachment';
    const thumb = document.createElement('img');
    thumb.alt = '';
    thumb.src = `data:${image.mime_type};base64,${image.data_base64}`;
    const caption = document.createElement('span');
    caption.textContent = `${image.name} \u00b7 ${image.width}\u00d7${image.height}`;
    const remove = document.createElement('button');
    remove.type = 'button';
    remove.className = 'danger';
    remove.setAttribute('aria-label', `Remove ${image.name}`);
    remove.textContent = '\u00d7';
    remove.onclick = () => {
      promptImages.splice(index, 1);
      renderAttachments();
    };
    chip.append(thumb, caption, remove);
    attachments.append(chip);
  }
}
async function submitPrompt() {
  if (!currentSession) return;
  const value = composerText(),
    images = promptImages;
  if (!value.trim() && !images.length) return;
  const error = document.querySelector('#conversation-error');
  if (value.startsWith('!') && images.length) {
    error.textContent = 'Shell commands cannot carry images.';
    return;
  }
  const body = value.startsWith('!')
    ? { action: 'run-shell', session_id: currentSession, command: value.slice(1) }
    : {
        action: 'prompt',
        session_id: currentSession,
        text: value,
        images: images.map(image => ({
          data_base64: image.data_base64,
          mime_type: image.mime_type,
          width: image.width,
          height: image.height,
        })),
      };
  const payload = JSON.stringify(body);
  if (new TextEncoder().encode(payload).byteLength > MAX_PROMPT_REQUEST_BYTES) {
    error.textContent = 'Prompt attachments exceed the 32 MiB request limit.';
    return;
  }
  try {
    await request('/api/actions', { method: 'POST', body: payload });
    setComposerText('');
    promptImages = [];
    renderAttachments();
    error.textContent = '';
    await refresh();
  } catch (err) {
    error.textContent = err.message;
  }
}
/// Roles whose body is prose the agent or the person wrote, and so is rendered
/// as Markdown. Everything else — tool output, plans, Hel's own notes — is
/// preformatted text, because Markdown in a tool dump is a coincidence rather
/// than an intent.
const PROSE_ROLES = new Set(['user', 'agent', 'thought']);

/// The glyph and label the terminal surface gives each role, so the two read
/// alike. `fn entry_visual` in `src/hel_chat/transcript.rs` is the original;
/// Milestone 3 carries the glyph in the projection so this copy can go.
const ROLE_GLYPH = {
  user: '\u276f',
  agent: '\u25cf',
  thought: '\u25cb',
  tool: '\u2022',
  plan: '\u25c7',
  'plan-proposal': '\u25c8',
  system: '\u2500',
};

function entryBody(entry) {
  const text = entry.lines.join('\n');
  if (PROSE_ROLES.has(entry.role)) {
    const body = el('div', 'entry-body');
    body.append(renderMarkdown(text));
    return body;
  }
  return el('pre', 'entry-body', text);
}

function renderEntries(entries, replace) {
  if (replace) {
    feed.replaceChildren();
    entryNodes.clear();
  }
  for (const entry of entries) {
    let node = entryNodes.get(entry.id);
    if (!node) {
      node = el('article');
      node.dataset.entryId = entry.id;
      entryNodes.set(entry.id, node);
      feed.append(node);
    }
    node.className = `entry ${entry.role}`;
    const heading = el('strong');
    const glyph = el('span', 'entry-glyph', ROLE_GLYPH[entry.role] || '\u2500');
    // The glyph repeats what the label already says, so it is decoration to a
    // screen reader and must not be read out twice.
    glyph.setAttribute('aria-hidden', 'true');
    heading.append(glyph, el('span', 'entry-label', entry.label));
    node.replaceChildren(heading, entryBody(entry));
  }
  window.scrollTo(0, document.body.scrollHeight);
}
async function loadConversation(delta = false) {
  if (!currentSession) return;
  try {
    const result = await request(
      `/api/conversations/${encodeURIComponent(currentSession)}${delta && cursor ? `?after_seq=${cursor}` : ''}`,
    );
    renderEntries(result.entries, !delta || result.reset);
    cursor = result.latest_seq;
    if (cursor > acknowledged) {
      const through = cursor;
      await request(`/api/conversations/${encodeURIComponent(currentSession)}/read`, {
        method: 'POST',
        body: JSON.stringify({ through }),
      });
      acknowledged = through;
    }
  } catch (err) {
    document.querySelector('#conversation-error').textContent = err.message;
  }
}
async function openConversation(id) {
  const session = snapshot?.sessions.find(x => x.id === id);
  if (!session?.conversation_available) {
    showDashboard();
    return;
  }
  currentSession = id;
  cursor = 0;
  acknowledged = 0;
  location.hash = `conversation/${id}`;
  dashboard.classList.add('hidden');
  conversation.classList.remove('hidden');
  document.querySelector('#conversation-title').textContent = session.title;
  document.querySelector('#conversation-state').textContent = session.state;
  renderQueue(session);
  renderElicitations(session);
  promptImages = [];
  renderAttachments();
  await loadConversation(false);
}
function showDashboard() {
  currentSession = null;
  cursor = 0;
  acknowledged = 0;
  location.hash = '';
  elicitations.replaceChildren();
  elicitationCards.clear();
  promptImages = [];
  renderAttachments();
  conversation.classList.add('hidden');
  dashboard.classList.remove('hidden');
}
document.querySelector('#login-form').onsubmit = async e => {
  e.preventDefault();
  try {
    await request('/auth/session', {
      method: 'POST',
      body: JSON.stringify({ code: document.querySelector('#code').value }),
    });
    document.querySelector('#login-error').textContent = '';
    await restoreRoute();
  } catch (err) {
    document.querySelector('#login-error').textContent = err.message;
  }
};
logout.onclick = async () => {
  await request('/auth/session', { method: 'DELETE' });
  location.reload();
};
newTarget.onchange = syncProjectDirectory;
newForm.onsubmit = async e => {
  e.preventDefault();
  const target = snapshot.targets.find(x => x.id === newTarget.value);
  try {
    await request('/api/actions', {
      method: 'POST',
      body: JSON.stringify({
        action: 'new',
        title: document.querySelector('#new-title').value,
        profile_id: newProfile.value,
        bundle_id: newBundle.value,
        target_id: newTarget.value,
        project_directory: target?.requires_project_directory ? newProjectDirectory.value : null,
      }),
    });
    document.querySelector('#new-title').value = '';
    actionError.textContent = '';
    await refresh();
  } catch (err) {
    actionError.textContent = err.message;
  }
};
sessions.onclick = async e => {
  const button = e.target.closest('button[data-action]');
  if (!button) return;
  if (button.dataset.action === 'open') return openConversation(button.dataset.id);
  if (
    button.dataset.action === 'close' &&
    !confirm(
      'Save a recovery copy, stop, and destroy this session target? Queued prompts will be preserved.',
    )
  )
    return;
  const body = { action: button.dataset.action, session_id: button.dataset.id };
  if (button.dataset.action === 'resume') {
    body.profile_id = button.dataset.profile;
    body.target_id = button.dataset.target;
    const session = snapshot.sessions.find(x => x.id === button.dataset.id);
    body.queue = 'start';
    if (session?.queued_prompts?.length) {
      const choice = prompt(
        `This session has ${session.queued_prompts.length} queued prompt(s). Type start to run them after resume, or discard to remove them.`,
        'start',
      );
      if (choice === null) return;
      if (!['start', 'discard'].includes(choice.toLowerCase()))
        return alert('Enter start or discard.');
      body.queue = choice.toLowerCase();
    }
  }
  try {
    await request('/api/actions', { method: 'POST', body: JSON.stringify(body) });
    actionError.textContent = '';
    await refresh();
  } catch (err) {
    actionError.textContent = err.message;
  }
};
document.querySelector('#back').onclick = showDashboard;
document.querySelector('#prompt-form').onsubmit = e => {
  e.preventDefault();
  submitPrompt();
};
promptText.addEventListener('input', composerInputChanged);
// Rich text, and anything a paste or drop would inject as markup, never
// belongs in a prompt: refuse it here and re-insert the plain text instead.
promptText.addEventListener('beforeinput', e => {
  const kind = e.inputType || '';
  if (
    kind === 'insertHTML' ||
    kind.startsWith('insertFromDrop') ||
    kind.startsWith('insertFromPaste') ||
    kind.startsWith('format')
  )
    e.preventDefault();
});
promptText.addEventListener('paste', e => {
  const files = Array.from(e.clipboardData?.items || [])
    .filter(item => item.kind === 'file' && item.type.startsWith('image/'))
    .map(item => item.getAsFile())
    .filter(Boolean);
  if (files.length) {
    e.preventDefault();
    const session = snapshot?.sessions.find(x => x.id === currentSession);
    if (session?.prompt_images_supported) attachImageFiles(files);
    else
      document.querySelector('#conversation-error').textContent =
        'This session does not support image prompts.';
    return;
  }
  const text = e.clipboardData?.getData('text/plain');
  if (text === undefined) return;
  e.preventDefault();
  insertComposerText(text);
});
promptText.addEventListener('dragover', e => {
  e.preventDefault();
  const types = Array.from(e.dataTransfer?.types || []);
  if (e.dataTransfer)
    e.dataTransfer.dropEffect = types.some(type => type === 'text/plain' || type === 'Files')
      ? 'copy'
      : 'none';
});
promptText.addEventListener('drop', e => {
  e.preventDefault();
  placeComposerCaretAtPoint(e.clientX, e.clientY);
  const files = Array.from(e.dataTransfer?.files || []).filter(file =>
    file.type.startsWith('image/'),
  );
  if (files.length) {
    const session = snapshot?.sessions.find(x => x.id === currentSession);
    if (session?.prompt_images_supported) attachImageFiles(files);
    else
      document.querySelector('#conversation-error').textContent =
        'This session does not support image prompts.';
    return;
  }
  const text = e.dataTransfer?.getData('text/plain') || '';
  if (text) insertComposerText(text);
});
// An active IME composition steers its candidate with Enter and the arrows,
// so the composer must not read those keys until the composition ends.
promptText.addEventListener('keydown', e => {
  if (e.isComposing || e.keyCode === 229) return;
  if (e.key === 'Enter' && !e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey) {
    e.preventDefault();
    submitPrompt();
    return;
  }
  if (e.key === 'Enter' && e.shiftKey && !e.metaKey && !e.ctrlKey && !e.altKey) {
    e.preventDefault();
    insertComposerLineBreak();
    return;
  }
  if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
    e.preventDefault();
    submitPrompt();
  }
});
attachImage.onclick = () => imagePicker.click();
imagePicker.onchange = () => {
  const files = Array.from(imagePicker.files || []);
  imagePicker.value = '';
  attachImageFiles(files);
};
queue.onclick = async e => {
  const button = e.target.closest('button[data-queue-id]');
  if (!button) return;
  try {
    await request('/api/actions', {
      method: 'POST',
      body: JSON.stringify({
        action: 'remove-queued-prompt',
        session_id: currentSession,
        queue_id: button.dataset.queueId,
      }),
    });
    await refresh();
  } catch (err) {
    document.querySelector('#conversation-error').textContent = err.message;
  }
};
shells.onclick = async e => {
  const button = e.target.closest('button[data-shell-id]');
  if (!button) return;
  try {
    await request('/api/actions', {
      method: 'POST',
      body: JSON.stringify({
        action: 'cancel-shell',
        session_id: currentSession,
        shell_command_id: button.dataset.shellId,
      }),
    });
    await refresh();
  } catch (err) {
    document.querySelector('#conversation-error').textContent = err.message;
  }
};
window.addEventListener('online', () => {
  startEvents();
  refresh();
});
if ('serviceWorker' in navigator) navigator.serviceWorker.register('/service-worker.js');
restoreRoute();

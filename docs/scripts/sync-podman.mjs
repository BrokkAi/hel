import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const docsRoot = fileURLToPath(new URL('..', import.meta.url));
const sourcePath = join(docsRoot, 'PODMAN.md');
const targetPath = join(docsRoot, 'src', 'content', 'docs', 'podman.md');

const heading = '# Podman for Hel';
const source = readFileSync(sourcePath, 'utf8');
if (!source.startsWith(`${heading}\n`)) {
  console.error(`${sourcePath} does not start with the expected heading ${JSON.stringify(heading)}.`);
  process.exit(1);
}
const body = source.slice(heading.length).replace(/^\s*\n/, '');

const frontmatter = [
  '---',
  'title: "Podman for Hel"',
  'description: "Rootless Podman installation, verification postconditions, and remediation for hel container targets."',
  '---',
  '',
  '',
].join('\n');

writeFileSync(targetPath, frontmatter + body);
console.log(`Synced ${sourcePath} -> ${targetPath}`);

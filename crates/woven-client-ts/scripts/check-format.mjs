import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const root = new URL("..", import.meta.url);
const directories = ["src", "test", "scripts"];
const extensions = new Set([".ts", ".mjs"]);
const files = [];

async function collect(directory) {
  for (const entry of await readdir(new URL(`${directory}/`, root), { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await collect(path);
    } else if (extensions.has(path.slice(path.lastIndexOf(".")))) {
      files.push(path);
    }
  }
}

for (const directory of directories) {
  await collect(directory);
}

const violations = [];
for (const file of files) {
  const source = await readFile(new URL(file, root), "utf8");
  if (!source.endsWith("\n")) {
    violations.push(`${file}: missing trailing newline`);
  }
  if (/[^\S\r\n]+$/m.test(source)) {
    violations.push(`${file}: trailing whitespace`);
  }
}

if (violations.length > 0) {
  console.error("Formatting violations:");
  for (const violation of violations) {
    console.error(`- ${violation}`);
  }
  process.exitCode = 1;
}

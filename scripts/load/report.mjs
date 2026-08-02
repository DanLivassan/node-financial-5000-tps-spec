import { readdir, readFile } from "node:fs/promises";

const directory = new URL("../../artifacts/load-tests/", import.meta.url);
const files = (await readdir(directory)).filter((file) => file.endsWith(".json")).sort();
if (files.length === 0) throw new Error("No load-test artifacts found");
for (const file of files) {
  const artifact = JSON.parse(await readFile(new URL(file, directory), "utf8"));
  const validity = artifact.summary.errors === 0 && artifact.summary.non2xx === 0 ? "VALID" : "INVALID";
  console.log(`${file} [${validity}]\n${JSON.stringify(artifact.summary, null, 2)}\n`);
}

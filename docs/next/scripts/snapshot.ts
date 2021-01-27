import { read, write } from "to-vfile";
import remark from "remark";
import mdx from "remark-mdx";
import codeTransformer from "../util/codeTransformer";

(async () => {
  const path = "./content/concepts/solids-pipelines/solids.mdx";
  const file = await read(path);
  const contents = await remark().use(mdx).use(codeTransformer).process(file);

  await write({
    path,
    contents: Buffer.from(contents.toString()),
  });
})();

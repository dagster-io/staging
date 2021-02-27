import fs from 'fs';
import path from 'path';

import glob from 'glob';
import { createCompiler } from '@mdx-js/mdx';

const { parse: parseMdxContent } = createCompiler();

const ROOT_DIR = path.resolve(__dirname, '../');
const DOCS_DIR = path.resolve(ROOT_DIR, 'content');

type MdxAstNode = any;

test('no dead links', async () => {
  const allMdxFilePaths = await findAllMdxFileRelativePaths();

  const astStore: { [filePath: string]: MdxAstNode } = {};
  const allInternalLinksStore: { [filePath: string]: Array<string> } = {};

  // parse mdx files to find all internal links and populate the store
  await Promise.all(
    allMdxFilePaths.map(async (relativeFilePath) => {
      const absolutePath = path.resolve(DOCS_DIR, relativeFilePath);
      const fileContent = await fs.promises.readFile(absolutePath, 'utf-8');
      astStore[relativeFilePath] = parseMdxContent(fileContent);
    }),
  );

  for (const filePath in astStore) {
    const internalLinks = collectInternalLinks(astStore[filePath], filePath);
    allInternalLinksStore[filePath] = internalLinks;
  }

  const allMdxFileSet = new Set(allMdxFilePaths);
  const deadLinks: Array<{ sourceFile: string; deadLink: string }> = [];

  let linkCount = 0;

  for (const source in allInternalLinksStore) {
    const linkList = allInternalLinksStore[source];

    for (const link of linkList) {
      linkCount++;
      if (!isLinkLegit(link, allMdxFileSet, astStore)) {
        deadLinks.push({
          sourceFile: path.resolve(DOCS_DIR, source),
          deadLink: link,
        });
      }
    }
  }

  // sanity check to make sure the parser is working
  expect(linkCount).toBeGreaterThan(0);

  expect(deadLinks).toEqual([]);
});

function getMatchCandidates(targetPath: string): Array<string> {
  return [`${targetPath}.mdx`, `${targetPath}/index.mdx`];
}

function isLinkLegit(
  rawTarget: string,
  allMdxFileSet: Set<string>,
  astStore: { [filePath: string]: MdxAstNode },
): boolean {
  // TODO: Validate links to API Docs
  if (rawTarget.startsWith('_apidocs/')) {
    return true;
  }

  // Validate links to public assets
  if (rawTarget.startsWith('assets/')) {
    return fileExists(path.resolve(ROOT_DIR, 'public', rawTarget));
  }

  // Validate regular content links
  if (!rawTarget.includes('#')) {
    // the link target doesn't have a "#" anchor
    return getMatchCandidates(rawTarget).some((name) =>
      allMdxFileSet.has(name),
    );
  }

  // TODO: Validate links with anchors
  const [target, anchor] = rawTarget.split('#');
  const targetFilePath = getMatchCandidates(target).find((name) =>
    allMdxFileSet.has(name),
  );
  if (targetFilePath) {
    return true
  }

  return false;
}

// recursively find all filepaths relative to `DOCS_DIR`
function findAllMdxFileRelativePaths(): Promise<Array<string>> {
  const options = {
    cwd: DOCS_DIR,
  };

  return new Promise((resolve, reject) => {
    // NOTE: assuming all sources are `.mdx` files
    glob('**/*.mdx', options, (error, files) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(files);
    });
  });
}

// traverse the mdx ast to find all internal links
function collectInternalLinks(
  rootAstNode: MdxAstNode,
  currentFilePath: string,
): Array<string> {
  const externalLinkRegex = /^https?:\/\//;

  const queue = [rootAstNode];
  const result: Array<string> = [];

  while (queue.length > 0) {
    const node = queue.shift();
    if (!node) {
      continue;
    }
    if (Array.isArray(node.children)) {
      queue.push(...node.children);
    }

    if (!((node.type === 'link' || node.type === 'image') && node.url)) {
      continue;
    }

    const { url } = node;
    if (url.match(externalLinkRegex)) {
      continue;
    }

    if (url.startsWith('#')) {
      // is a self-referencing anchor link
      result.push(`${currentFilePath.replace(/\.mdx$/, '')}${url}`);
    } else if (!url.startsWith('/')) {
      // TODO links should all be absolute paths
      // throw new Error(
      //   `Do not use relative references ('${url}' in ${currentFilePath}). All links should start with '/'`,
      // );
    } else {
      // remove the leading `/` from the link target
      result.push(url.substr(1));
    }
  }

  return result;
}

function fileExists(filePath: string): boolean {
  try {
    fs.statSync(filePath);
    return true;
  } catch (_) {
    return false;
  }
}
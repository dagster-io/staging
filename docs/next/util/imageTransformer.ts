import { Node } from "hast";
import path from "path";
import visit from "unist-util-visit";
const sizeOf = require("image-size");

const PUBLIC_DIR = path.join(__dirname, "../public/");

interface ParentNode extends Node {
  type: string;
  children: Node[];
}
interface ImageNode extends Node {
  type: string;
  url: string;
  alt: string;
  value: string;
}

export interface ImageStats {
  totalImages: number;
  updatedImages: string[];
}

interface ImageTransformerOptions {
  setImageStats?: (newStats: ImageStats) => void;
}

// export default ({ setImageStats }: ImageTransformerOptions) => async (
//   tree: Node
// ) => {
//   const stats: ImageStats = {
//     updatedImages: [],
//   };
//   const parentNodes: ParentNode[] = [];

//   visit(tree, "paragraph", (node: ParentNode) => {
//     if (node.children.some((n) => n.type === "image")) {
//       parentNodes.push(node);
//     }
//   });

//   for (const node of parentNodes) {
//     const imageNode: ImageNode = node.children.find(
//       (n) => n.type === "image"
//     ) as any;
//     console.log(imageNode);

//     // Skip external images
//     if (imageNode.url.startsWith("/")) {
//       const fileAbsPath = path.join(PUBLIC_DIR, imageNode.url);
//       const dimensions = sizeOf(fileAbsPath);

//       // Convert original node to Image
//       imageNode.type = "html";
//       imageNode.value = `<Image alt="${imageNode.alt}" src="${imageNode.url}" width={${dimensions.width}} height={${dimensions.height}} />`;

//       node.children = [imageNode];

//       stats.updatedImages.push(imageNode.value as string);
//     }
//   }

//   if (setImageStats) {
//     setImageStats(stats);
//   }
// };

export default ({ setImageStats }: ImageTransformerOptions) => async (
  tree: Node
) => {
  const stats: ImageStats = {
    totalImages: 0,
    updatedImages: [],
  };
  const images: ImageNode[] = [];

  visit(tree, ["image", "html"], (node: ImageNode) => {
    images.push(node);
  });

  for (const node of images) {
    console.log(node);

    // Skip external images
    if (node.url.startsWith("/")) {
      stats.totalImages++;
      const fileAbsPath = path.join(PUBLIC_DIR, node.url);
      const dimensions = sizeOf(fileAbsPath);
      const imageValue = `<Image alt="${node.alt}" src="${node.url}" width={${dimensions.width}} height={${dimensions.height}} />`;

      // Convert original node to Image
      if (node.value !== imageValue) {
        node.type = "html";
        node.value = imageValue;
        stats.updatedImages.push(node.value as string);
      }
    }
  }

  if (setImageStats) {
    setImageStats(stats);
  }
};

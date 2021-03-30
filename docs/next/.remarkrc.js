exports.settings = {
  bullet: "-",
  emphasis: "_",
  strong: "*",
  listItemIndent: "one",
  rule: "-",
};
exports.plugins = [
  require("remark-frontmatter"),
  require("remark-preset-prettier"),
  // currently disable GitHub Flavored Markdown's table pipe alignment bc of super long cells
  [require("remark-gfm"), { tablePipeAlign: false }],
];

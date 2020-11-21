module.exports = {
  async redirects() {
    return [
      {
        source: "/docs",
        destination: "/docs/latest",
        permanent: true,
      },
    ];
  },
  i18n: {
    // These are all the locales you want to support in
    // your application
    locales: ["latest", "master", "0.9.20", "0.9.19"],
    // This is the default locale you want to be used when visiting
    // a non-locale prefixed path e.g. `/hello`
    defaultLocale: "latest",
  },
};

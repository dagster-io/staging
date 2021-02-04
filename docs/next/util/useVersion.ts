const ALL_VERSIONS = ["0.9.19", "0.9.20", "0.9.21", "master"];
const defaultVersion = "master";

import React, { useEffect, useState } from "react";

import { useRouter } from "next/router";

export function normalizeVersionPath(
  asPath: string,
  versions?: string[]
): {
  version?: string;
  asPath: string;
  versions: string[];
  defaultVersion: string;
} {
  let detectedVersion: string = defaultVersion;
  // first item will be empty string from splitting at first char
  const pathnameParts = asPath.split("/");

  (versions || []).some((version) => {
    if (pathnameParts[1].toLowerCase() === version.toLowerCase()) {
      detectedVersion = version;
      pathnameParts.splice(1, 1);
      asPath = pathnameParts.join("/") || "/";
      return true;
    }
    return false;
  });

  return {
    asPath,
    version: detectedVersion,
    versions: ALL_VERSIONS,
    defaultVersion,
  };
}

export enum SphinxPrefix {
  API_DOCS = "_apidocs",
  MODULES = "_modules",
}

const SPHINX_PREFIXES = [SphinxPrefix.API_DOCS, SphinxPrefix.MODULES];

export function normalizeSphinxPath(
  asPath: string,
  sphinxPrefixes?: string[]
): {
  sphinxPrefix?: SphinxPrefix;
  asPath: string;
} {
  let detectedPrefix: SphinxPrefix | undefined;
  // first item will be empty string from splitting at first char
  const pathnameParts = asPath.split("/");

  (sphinxPrefixes || []).some((prefix) => {
    if (pathnameParts[1].toLowerCase() === prefix.toLowerCase()) {
      detectedPrefix = prefix as SphinxPrefix;
      pathnameParts.splice(1, 1);
      asPath = pathnameParts.join("/") || "/";
      return true;
    }
    return false;
  });

  return {
    asPath,
    sphinxPrefix: detectedPrefix,
  };
}

export function versionFromPage(page: string | string[]) {
  if (Array.isArray(page)) {
    return normalizeVersionPath("/" + page.join("/"), ALL_VERSIONS);
  }

  return normalizeVersionPath(page, ALL_VERSIONS);
}

export function sphinxPrefixFromPage(page: string | string[]) {
  if (Array.isArray(page)) {
    return normalizeSphinxPath("/" + page.join("/"), SPHINX_PREFIXES);
  }

  return normalizeSphinxPath(page, SPHINX_PREFIXES);
}

export const useVersion = () => {
  const router = useRouter();

  const [asPath, setAsPath] = useState("/");

  useEffect(() => {
    if (router.isReady) {
      setAsPath(router.asPath);
    }
  }, [router]);

  return normalizeVersionPath(asPath, ALL_VERSIONS);
};

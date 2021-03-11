import { Octokit } from "@octokit/rest";
import { createAppAuth } from "@octokit/auth-app";

const octokit = new Octokit({
  authStrategy: createAppAuth,
  auth: {
    appId: process.env.GITHUB_APP_ID,
    privateKey: Buffer.from(process.env.GITHUB_PRIVATE_KEY, "base64"),
    clientId: process.env.GITHUB_PRIVATE_KEY,
    installationId: process.env.GITHUB_INSTALLATION_ID,
    clientSecret: process.env.GITHUB_CLIENT_SECRET,
  },
});

export const createGithubIssue = async ({
  title,
  body,
}: {
  title: string;
  body: string;
}) => {
  const issue = await octokit.issues.create({
    owner: "helloworld",
    repo: "test-repo",
    title,
    body,
  });

  return issue.data.html_url;
};

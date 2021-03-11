import { App, AppMentionEvent, LogLevel } from "@slack/bolt";

import { WebClient } from "@slack/web-api";
import { createGithubIssue } from "./github";

const TRIGGER = "docs issue";

const repliesAsString = async (
  client: WebClient,
  channel: string,
  thread_ts: string
) => {
  const replies = await client.conversations.replies({
    channel,
    ts: thread_ts,
  });
  const { messages } = replies;

  const messageText = messages.map(
    (message) => `${message.user}: ${message.text}`
  );
  return messageText.join("\n");
};

const threadTimeStampFromEvent = (event: AppMentionEvent) => {
  return event.thread_ts ? event.thread_ts : event.ts;
};

const getContextFromEvent = async (event: AppMentionEvent) => {
  const thread_ts = threadTimeStampFromEvent(event);
  const channel = event.channel;

  return { thread_ts, channel };
};

export default function (receiver) {
  const app = new App({
    receiver,
    token: process.env.SLACK_BOT_TOKEN,
    logLevel: LogLevel.DEBUG,
  });

  app.event("app_mention", async ({ event, message, say, client }) => {
    if (event.text.includes(TRIGGER)) {
      const { channel, thread_ts } = await getContextFromEvent(event);
      const message = event.text.substring(
        event.text.indexOf(TRIGGER) + TRIGGER.length + 1
      );
      const repliesString = await repliesAsString(client, channel, thread_ts);
      const { permalink } = await client.chat.getPermalink({
        channel,
        message_ts: thread_ts,
      });

      const githubIssueBody = `
## What content is missing
<!-- Help us to understand your request in context -->

${message}

This issue was generated from the Slack conversation at: ${permalink}

Conversation excerpt:

\`\`\`
${repliesString}
\`\`\`

## Type of the content
<!--
Among the following items, pick a doc type that the content would most likely belong to
- Tutorial
- Main Concept: "What is X in Dagster" where X is a Dagster concept.
- Integration Guide: "How to use X in Dagster" where X is a 3rd-party package/tool, e.g. dbt, Spark, etc
- Best Practices Guide: "How to do X in Dagster" where X is usually a use case and can be addressed by incorporating multiple Dagster concepts at a time.
- Deployment Guide: "How to deploy Dagster to X", e.g. Docker, K8S, GCP, etc
- API Reference: e.g. docstring
-->

## (Optional) Anything in particular you want the docs to cover
<!-- Do you have exactly anything in mind you are looking for
Examples:
- Show how to test a partition set
- Include a diagram to explain the relations between inputs and outputs
-->


## (Optional) Target Date
<!-- When do you want to have this by -->

## Writer's Guide
- [Docs README](https://github.com/dagster-io/dagster/tree/master/docs)
- Templates to follow: <fill by the docs team>
- Examples for reference: <fill by the docs team>

---
#### Message from the maintainers:
Are you looking for the same documentation content? Give it a :thumbsup:. We factor engagement into prioritization.
      `;

      const issue = await createGithubIssue({
        title: `[Content Gap] ${message}`,
        body: githubIssueBody,
      });

      say({ text: `Created issue at: ${issue}`, thread_ts });
    }
  });
}

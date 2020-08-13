import React from 'react';
import Head from 'next/head';

type Props = {
  title?: string;
  description?: string;
};

export function DynamicMetaTags(props: Props) {
  const {
    title = 'Dagster — A Python library for building data applications',
    description = 'Dagster is a data orchestrator for machine learning, analytics, and ETL',
  } = props;

  return (
    <Head>
      <title key="real-title">{title}</title>
      <meta key="title" name="title" content={title} />
      <meta key="og:title" property="og:title" content={title} />
      <meta key="twitter:title" property="twitter:title" content={title} />

      <meta key="description" name="description" content={description} />
      <meta
        key="og:description"
        property="og:description"
        content={description}
      />
      <meta
        key="twitter:description"
        property="twitter:description"
        content={description}
      />
    </Head>
  );
}

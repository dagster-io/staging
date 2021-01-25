import Head from "next/head";
import Link from "next/link";
import { useState } from "react";
import {
  Application,
  getApplication,
  getReview,
  listReviewers,
  listReviewsForReviewer,
  Review,
} from "util/airtable";
import cx from "classnames";

interface ReviewPageProps {
  review: Review;
  application: any;
}

function hashCode(str) {
  return str
    .split("")
    .reduce(
      (prevHash, currVal) =>
        ((prevHash << 5) - prevHash + currVal.charCodeAt(0)) | 0,
      0
    );
}

const getColorForString = (s: string) => {
  const colors = [
    ["bg-yellow-100", "text-yellow-800"],
    ["bg-green-100", "text-green-800"],
    ["bg-blue-100", "text-blue-800"],
    ["bg-red-100", "text-red-800"],
    ["bg-indigo-100", "text-indigo-800"],
    ["bg-pink-100", "text-pink-800"],
    ["bg-purple-100", "text-purple-800"],
  ];

  return colors[Math.abs(hashCode(s)) % colors.length];
};

const ValueRenderer = ({ type, value }) => {
  switch (type) {
    case "string":
      return <p>{value}</p>;
    case "link":
      return <a href={value}>{value}</a>;
    case "text":
      return <p>{value}</p>;
    case "dropdown":
      return value.map((v) => {
        const pair = getColorForString(v);
        console.log(pair);
        const bgColor = pair[0];
        const textColor = pair[1];
        return (
          <span
            className={`inline-flex mr-2 items-center px-2.5 py-0.5 rounded-md text-sm font-medium ${bgColor} ${textColor}`}
          >
            {v}
          </span>
        );
      });
    default:
      return <h1>UNSUPPORTED TYPE</h1>;
  }
};

export default function ReviewPage({ review, application }: ReviewPageProps) {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-red-100 px-8 py-4">
      <Head>
        <title>Network</title>
        <link rel="icon" href="/favicon.ico" />
      </Head>

      {/* Header  */}
      <div className="flex items-baseline pt-4 pb-8">
        <h1 className="font-bold text-xl">Review for {review.name}</h1>
        <a className="underline ml-2" href={"/"}>
          (go back)
        </a>
      </div>

      {/* Content */}
      <div className="flex flex-1 space-x-12">
        <div className="bg-gray-100 prose max-w-4xl p-4 overflow-scroll">
          <h2 className="font-bold">Application</h2>

          {Object.entries(application).map(([key, value]) => {
            return (
              <div className="mb-2 bg-gray-50 border px-4 pb-4 border-gray-300">
                <p className=" font-bold">{key}</p>
                <ValueRenderer type={value.type} value={value.value} />
              </div>
            );
          })}
        </div>

        <div className="bg-gray-50 p-4 flex-1">
          <h2 className="font-bold">Review by {review.reviewer}</h2>

          <p className="mt-2">Scores:</p>

          <ul className="list-disc list-inside">
            <li>Mastery: {review.scores.Mastery}</li>
            <li>Mission: {review.scores.Mission}</li>
            <li>Integrity: {review.scores.Integrity}</li>
            <li>Individuality: {review.scores.Individuality}</li>
          </ul>

          <p className="mt-2">Notes:</p>
          <textarea>{review.notes}</textarea>

          <p className="mt-2">What would you look for next year:</p>
          <textarea>{review.nextYearNotes}</textarea>
        </div>
      </div>
    </div>
  );
}

export async function getServerSideProps(context) {
  const { id: reviewId } = context.params;
  const review = await getReview(reviewId);
  const application = await getApplication(review.applicationId);

  return {
    props: { review, application },
  };
}

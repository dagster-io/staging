import { promises as fs } from "fs";
import path from "path";
import Sidebar from "../sidebar";

const ApiDocsPage = ({ body, data, page, curr }) => {
  const markup = { __html: body };

  return (
    <div className="max-w-6xl mx-auto">
      <div className="flex space-x-16">
        <div className="bg-gray-100 w-64">
          <Sidebar />
        </div>
        <div className="prose prose-sm" dangerouslySetInnerHTML={markup} />
      </div>
    </div>
  );
};

export async function getServerSideProps({ params, locale }) {
  const { page } = params;

  const pathToFile = path.resolve("content", locale, "data/sections.json");

  try {
    const buffer = await fs.readFile(pathToFile);
    const {
      api: { apidocs: data },
    } = JSON.parse(buffer.toString());

    let curr = data;
    for (const part of page) {
      curr = curr[part];
    }

    const { body } = curr;

    return {
      props: { body, data, page, curr },
    };
  } catch (err) {
    console.log(err);
    return {
      notFound: true,
    };
  }
}

export default ApiDocsPage;

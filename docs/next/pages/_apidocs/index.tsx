import Sidebar from "../sidebar";

const APIDocsIndex = () => {
  return (
    <div className="max-w-6xl mx-auto">
      <div className="flex space-x-16">
        <div className="bg-gray-100 w-64">
          <Sidebar />
        </div>
        <div className="prose prose-sm">
          <h1>Hello</h1>
        </div>
      </div>
    </div>
  );
};

export default APIDocsIndex;

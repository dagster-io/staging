import { useRouter } from "next/router";

interface TableOfContents {
  name: string;
  path: string;
  children?: TableOfContents[];
}

const Core: TableOfContents = {
  name: "Core",
  path: "/_apidocs#core",
  children: [
    {
      name: "Solids",
      path: "/_apidocs/solids",
    },
    {
      name: "Pipelines",
      path: "/_apidocs/pipeline",
    },
  ],
};

const Libraries: TableOfContents = {
  name: "Libraries",
  path: "/_apidocs#libraries",
  children: [
    {
      name: "Dagstermill",
      path: "/_apidocs/libraries/dagstermill",
    },
    {
      name: "Airflow (dagster_airflow)",
      path: "/_apidocs/libraries/dagster_airflow",
    },
  ],
};

const API: TableOfContents[] = [Core, Libraries];

const Sidebar = () => {
  return (
    <div>
      {API.map((section) => (
        <SidebarSection section={section} />
      ))}
    </div>
  );
};

interface SidebarSectionProps {
  section: TableOfContents;
}

const SidebarSection: React.FC<SidebarSectionProps> = ({ section }) => {
  const { asPath } = useRouter();
  const { name, path, children } = section;

  return (
    <div>
      <a className={path === asPath ? "bg-blue-100" : ""} href={path}>
        {name}
      </a>
      {children && (
        <div className="ml-5">
          {children.map((child) => (
            <SidebarSection section={child} />
          ))}
        </div>
      )}
    </div>
  );
};

export default Sidebar;

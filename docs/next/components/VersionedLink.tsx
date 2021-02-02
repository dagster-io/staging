import Link from "next/link";
import { useRouter } from "next/router";

interface VersionedLinkProps {
  href: string;
  children: JSX.Element;
}

const VersionedLink = ({ href, children }: VersionedLinkProps) => {
  // safely pass undefined router at the first render
  const router = useRouter();
  if (!router) {
    return null;
  }

  return (
    <Link href={href} locale={router.locale}>
      {children}
    </Link>
  );
};

export default VersionedLink;

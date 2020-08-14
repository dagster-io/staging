import { getAllPosts } from 'lib/apiBlogPosts';
import NextLink from 'next/link';

function Blog({ allPosts }: { allPosts: any }) {
  return (
    <ul className="divide-y divide-gray-200">
      {allPosts.map((post: any) => {
        const { slug, title, excerpt, coverImage, date } = post;
        if (!post.excerpt) {
          throw new Error(
            'Please provide a short extract (i.e. excerpt) for post: ' + title,
          );
        }

        return (
          <li key={slug} className="blogpost py-12 list-none">
            <article className="space-y-2 xl:grid xl:grid-cols-4 xl:space-y-0 xl:items-baseline">
              <dl>
                <dt className="sr-only">Published on</dt>
                <dd className="text-base leading-6 font-medium text-gray-500">
                  {date}
                </dd>
              </dl>
              <div className="space-y-5 xl:col-span-3">
                <div className="space-y-6">
                  <h2 className="text-2xl leading-4 mb-4 font-bold tracking-tight">
                    <NextLink href={'/blog/posts/' + post.slug}>
                      <a className="hover:text-blue-800">{post.title}</a>
                    </NextLink>
                  </h2>
                  <img src={coverImage} />

                  <div className="prose max-w-none text-gray-500">
                    {excerpt}
                  </div>
                </div>
                <div className="text-base leading-6 font-medium">
                  <NextLink href={'/blog/posts/' + post.slug}>
                    <a
                      className="hover:text-blue-800"
                      aria-label={`Read "${title}"`}
                    >
                      Read more &rarr;
                    </a>
                  </NextLink>
                </div>
              </div>
            </article>
          </li>
        );
      })}
    </ul>
  );
}

export async function getStaticProps() {
  const allPosts = getAllPosts([
    'slug',
    'title',
    'excerpt',
    'coverImage',
    'date',
  ]);

  return {
    props: { allPosts },
  };
}

export default Blog;

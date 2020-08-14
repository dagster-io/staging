import { getAllPosts } from 'lib/apiBlogPosts';

function Blog({ allPosts }: { allPosts: any }) {
  return (
    <div>
      {allPosts.map((post: any) => (
        <div>
          <a href={'/blog/posts/' + post.slug}>
            <h2>{post.title}</h2>
          </a>

          <p>
            <img src={post.coverImage} />
          </p>
          <p>{post.date}</p>
          <p>
            {
              // if a short extract is not provided, we generate it from the content
              post.excerpt ? post.excerpt : post.content.substr(0, 100) + '...'
            }
          </p>
        </div>
      ))}
    </div>
  );
}

export async function getStaticProps() {
  const allPosts = getAllPosts([
    'slug',
    'title',
    'excerpt',
    'coverImage',
    'date',
    'content',
  ]);

  return {
    props: { allPosts },
  };
}

export default Blog;

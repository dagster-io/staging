import {MockList} from '@graphql-tools/mock';
import {render, screen, waitFor} from '@testing-library/react';
import * as React from 'react';
import {MemoryRouter} from 'react-router-dom';

import {LeftNavRepositorySection, REPO_KEYS} from 'src/nav/LeftNavRepositorySection';
import {ApolloTestProvider} from 'src/testing/ApolloTestProvider';
import {WorkspaceProvider} from 'src/workspace/WorkspaceContext';

describe('Repository options', () => {
  const defaultMocks = {
    RepositoryLocationsOrError: () => ({
      __typename: 'RepositoryLocationConnection',
    }),
    RepositoryLocationConnection: () => ({
      nodes: () => new MockList(1),
    }),
    RepositoryLocationOrLoadFailure: () => ({
      __typename: 'RepositoryLocation',
    }),
    RepositoryLocation: () => ({
      name: () => 'bar',
      repositories: () => new MockList(1),
    }),
    SchedulesOrError: () => ({
      __typename: 'Schedules',
    }),
    Schedules: () => ({
      results: () => new MockList(1),
    }),
    SensorsOrError: () => ({
      __typename: 'Sensors',
    }),
    Sensors: () => ({
      results: () => new MockList(1),
    }),
  };

  it('Corrctly displays the current repository state', async () => {
    const mocks = {
      ...defaultMocks,
      Repository: () => ({
        name: () => 'foo',
        pipelines: () => new MockList(1),
      }),
      Pipeline: () => ({
        id: () => 'my_pipeline',
        name: () => 'my_pipeline',
      }),
    };

    render(
      <ApolloTestProvider mocks={mocks}>
        <MemoryRouter initialEntries={['/workspace/foo@bar/etc']}>
          <WorkspaceProvider>
            <LeftNavRepositorySection />
          </WorkspaceProvider>
        </MemoryRouter>
      </ApolloTestProvider>,
    );

    await waitFor(() => {
      expect(
        screen.getByRole('link', {
          name: /my_pipeline/i,
        }),
      ).toBeVisible();
    });
  });

  describe('localStorage', () => {
    beforeEach(() => {
      window.localStorage.clear();
    });

    const locationOne = 'ipsum';
    const repoOne = 'lorem';
    const locationTwo = 'bar';
    const repoTwo = 'foo';

    const mocks = {
      ...defaultMocks,
      RepositoryLocationConnection: () => ({
        nodes: () => [
          {
            __typename: 'RepositoryLocation',
            name: locationOne,
            repositories: () =>
              new MockList(1, () => ({
                name: repoOne,
                pipelines: () => new MockList(2),
              })),
          },
          {
            __typename: 'RepositoryLocation',
            name: locationTwo,
            repositories: () =>
              new MockList(1, () => ({
                name: repoTwo,
                pipelines: () => new MockList(4),
              })),
          },
        ],
      }),
    };

    it('initializes for all repo options, if no localStorage', async () => {
      render(
        <ApolloTestProvider mocks={mocks}>
          <MemoryRouter initialEntries={['/instance/runs']}>
            <WorkspaceProvider>
              <LeftNavRepositorySection />
            </WorkspaceProvider>
          </MemoryRouter>
        </ApolloTestProvider>,
      );

      await waitFor(() => {
        expect(screen.getAllByRole('link')).toHaveLength(6);
      });
    });

    it('correctly initializes from localStorage', async () => {
      window.localStorage.setItem(REPO_KEYS, '["foo:bar"]');
      render(
        <ApolloTestProvider mocks={mocks}>
          <MemoryRouter initialEntries={['/instance/runs']}>
            <WorkspaceProvider>
              <LeftNavRepositorySection />
            </WorkspaceProvider>
          </MemoryRouter>
        </ApolloTestProvider>,
      );

      // Initialize to `foo@bar`, which has four pipelines.
      await waitFor(() => {
        expect(screen.getAllByRole('link')).toHaveLength(4);
      });
    });
  });
});

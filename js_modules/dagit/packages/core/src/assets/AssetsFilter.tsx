import {
  InputGroup as BlueprintInputGroup,
  Colors,
  Icon,
  Popover,
  Tag as BlueprintTag,
} from '@blueprintjs/core';
import Fuse from 'fuse.js';
import memoize from 'lodash/memoize';
import * as React from 'react';
import {useHistory} from 'react-router-dom';
import styled from 'styled-components/macro';

import {Box} from '../ui/Box';
import {Group} from '../ui/Group';

import {AssetsTableQuery_assetsOrError_AssetConnection_nodes} from './types/AssetsTableQuery';

type Asset = AssetsTableQuery_assetsOrError_AssetConnection_nodes;

enum SuggestionType {
  TOKEN = 'TOKEN',
  VALUE = 'VALUE',
  FILTER = 'FILTER',
  ASSET = 'ASSET',
}

export interface TagFilter {
  key: string;
  value: string;
}

const tokenize = (tag: TagFilter) => {
  const {key, value} = tag;
  return `${key}=${value}`;
};

const getAssetFilterProviders = memoize((assets: Asset[] = [], tagFilters: TagFilter[] = []) => {
  const allTags = {};
  assets.forEach((asset) => {
    asset.tags.forEach((tag) => {
      allTags[tag.key] = tag.value;
    });
  });
  const tokenizedFilters = new Set(tagFilters.map(tokenize));
  const tagList = Object.keys(allTags)
    .map((key) => tokenize({key, value: allTags[key]}))
    .filter((x) => !tokenizedFilters.has(x));

  console.log('ASSET PROV', tagList);
  tagList.sort();

  if (!tagList.length) {
    return [];
  }

  return [
    {
      token: 'tag',
      values: () => tagList,
    },
  ];
});

export const filterAssets = (
  assets: Asset[],
  queryFilters?: string[],
  tagFilters?: TagFilter[],
) => {
  const matching = assets.filter(
    (asset) =>
      !tagFilters ||
      tagFilters.every((filterTag) =>
        asset.tags.some(
          (assetTag) => assetTag.key === filterTag.key && assetTag.value === filterTag.value,
        ),
      ),
  );

  if (!queryFilters || !queryFilters.length) {
    return matching;
  }

  return matching.filter((asset) =>
    queryFilters.every((query) => textMatches(asset.key.path.join(' '), query)),
  );
};

const textMatches = (haystack: string, needle: string) =>
  needle
    .toLowerCase()
    .split(' ')
    .filter((x) => x)
    .every((word) => haystack.toLowerCase().includes(word));

const fuseOptions = {threshold: 0.3};

type Suggestion = {
  type: SuggestionType;
  text: string;
  token?: string;
  asset?: Asset;
};

export const AssetsFilter = ({
  assets,
  tagFilters,
  queryFilters,
  onAddQueryFilter,
  onAddTagFilter,
}: {
  assets: Asset[];
  queryFilters: string[];
  tagFilters: TagFilter[];
  onAddQueryFilter: (query: string) => void;
  onAddTagFilter: (filter: TagFilter) => void;
}) => {
  const [highlight, setHighlight] = React.useState<number>(0);
  const [shown, setShown] = React.useState<boolean>(false);
  const [filterText, setFilterText] = React.useState('');
  const history = useHistory();

  const suggestionProviders = getAssetFilterProviders(assets);
  const {empty, perProvider} = React.useMemo(() => {
    const perProvider = suggestionProviders.reduce((accum, provider) => {
      const values = provider.values();
      return {...accum, [provider.token]: {fuse: new Fuse(values, fuseOptions), all: values}};
    }, {} as {[token: string]: {fuse: Fuse<string>; all: string[]}});
    const providerKeys = suggestionProviders.map((provider) => provider.token);
    return {
      empty: new Fuse(providerKeys, fuseOptions),
      perProvider,
    };
  }, [suggestionProviders]);

  const assetFuse = new Fuse<Asset>(assets, {
    threshold: 0.3,
    keys: ['key.path', 'tags.key', 'tags.value'],
  });

  const tokenizedFilters = new Set(tagFilters.map(tokenize));

  const getAssetSuggestions = (queryString: string): Suggestion[] => {
    return assetFuse
      .search(queryString)
      .map((result) => {
        const asset = result.item as Asset;
        return {
          type: SuggestionType.ASSET,
          text: asset.key.path.join(' › '),
          asset,
        };
      })
      .filter((result) =>
        Array.from(tokenizedFilters).every((filterString) =>
          result.asset.tags.map(tokenize).includes(filterString),
        ),
      );
  };
  const getTokenSuggestions = (queryString: string): Suggestion[] => {
    return empty
      .search(queryString)
      .map((result) => ({
        text: result.item,
        type: SuggestionType.TOKEN,
      }))
      .filter((suggestion) => suggestion.text !== queryString)
      .filter(
        (suggestion) =>
          perProvider[suggestion.text].all.filter((x) => !tokenizedFilters.has(x)).length,
      );
  };
  const getProviderSuggestions = (queryString: string): Suggestion[] => {
    if (!queryString) {
      return [];
    }

    const [token, value] = queryString.split(':');
    if (token in perProvider) {
      const {fuse, all} = perProvider[token];
      const results = value ? fuse.search(value).map((result) => result.item) : all;
      return results
        .filter((x) => !tokenizedFilters.has(x))
        .map((result) => `${token}:${result}`)
        .filter((x) => x.toLowerCase() !== queryString)
        .map((result) => ({text: result, type: SuggestionType.VALUE, token}));
    }

    return [];
  };

  const buildSuggestions = (queryFilters: string[], queryString: string): Suggestion[] => {
    const tokenSuggestions = getTokenSuggestions(queryString);
    const providerSuggestions = getProviderSuggestions(queryString);
    const assetSuggestions = getAssetSuggestions([...queryFilters, queryString].join(' '));
    const filterSuggestions =
      queryString && !queryFilters.includes(queryString)
        ? [
            {
              text: queryString,
              type: SuggestionType.FILTER,
            },
          ]
        : [];
    return [...filterSuggestions, ...tokenSuggestions, ...providerSuggestions, ...assetSuggestions];
  };

  const suggestions = buildSuggestions(queryFilters, filterText || '').slice(0, 6);
  const onSelect = React.useCallback(
    (suggestion: Suggestion) => {
      if (suggestion.type === SuggestionType.ASSET && suggestion.asset) {
        setFilterText('');
        history.push(
          `/instance/assets/${suggestion.asset.key.path.map(encodeURIComponent).join('/')}`,
        );
      } else if (suggestion.type === SuggestionType.VALUE && suggestion.token === 'tag') {
        const [_, x] = suggestion.text.split(':');
        const [key, value] = x.split('=');
        onAddTagFilter({key, value});
        setFilterText('');
      } else if (suggestion.type === SuggestionType.FILTER) {
        setFilterText('');
        setShown(false);
        onAddQueryFilter(suggestion.text);
      } else {
        setFilterText(suggestion.text);
      }
      setHighlight(0);
    },
    [onAddQueryFilter, onAddTagFilter, history],
  );

  const onKeyDown = (e: React.KeyboardEvent<any>) => {
    // Enter and Return confirm the currently selected suggestion or
    // confirm the freeform text you've typed if no suggestions are shown.
    if (e.key === 'Enter' || e.key === 'Return' || e.key === 'Tab') {
      if (suggestions.length) {
        const picked = suggestions[highlight];
        if (!picked) {
          throw new Error('Selection out of sync with suggestions');
        }
        onSelect(picked);
        e.preventDefault();
        e.stopPropagation();
      }
      return;
    }

    // Escape closes the options. The options re-open if you type another char or click.
    if (e.key === 'Escape') {
      setShown(false);
      setHighlight(0);
      return;
    }

    setShown(true);

    const lastResult = suggestions.length - 1;
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      setHighlight(highlight === 0 ? lastResult : highlight - 1);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      setHighlight(highlight === lastResult ? 0 : highlight + 1);
    }
  };

  const onChange = (e: React.ChangeEvent<any>) => {
    setFilterText(e.target.value);
    setHighlight(0);
  };

  const isOpen = shown && suggestions.length > 0;
  const filterSuggestions = suggestions.filter((x) => !x.asset);
  const assetSuggestions = suggestions.filter((x) => !!x.asset);

  return (
    <Popover
      minimal
      usePortal
      isOpen={isOpen}
      position={'bottom-left'}
      content={
        <Menu>
          {filterSuggestions.length ? (
            <Box border={{color: Colors.LIGHT_GRAY1, side: 'bottom', width: 4}}>
              {filterSuggestions.map((suggestion, idx) => (
                <SuggestionOption
                  key={idx}
                  suggestion={suggestion}
                  onSelect={onSelect}
                  isHighlighted={highlight === idx}
                />
              ))}
            </Box>
          ) : null}
          {assetSuggestions.map((suggestion, idx) => (
            <SuggestionOption
              key={idx}
              suggestion={suggestion}
              onSelect={onSelect}
              isHighlighted={highlight === idx + filterSuggestions.length}
            />
          ))}
        </Menu>
      }
    >
      <InputGroup
        type="text"
        value={filterText}
        width={300}
        fill={false}
        placeholder={`Select an asset or asset filter...`}
        hasSuggestions={isOpen}
        onChange={onChange}
        onKeyDown={onKeyDown}
        onBlur={() => setShown(false)}
        onFocus={() => setShown(true)}
      />
    </Popover>
  );
};

const SuggestionOption = ({
  suggestion,
  onSelect,
  isHighlighted,
}: {
  suggestion: Suggestion;
  onSelect: (suggestion: Suggestion) => void;
  isHighlighted: boolean;
}) => {
  const {text, type} = suggestion;

  const _content = () => {
    switch (type) {
      case SuggestionType.TOKEN:
        return (
          <Group direction="row" spacing={4} alignItems="center">
            <Icon
              iconSize={16}
              icon="filter-list"
              color={isHighlighted ? Colors.WHITE : Colors.GRAY3}
            />
            <div>Filter by {text}</div>
          </Group>
        );
      case SuggestionType.ASSET:
        return (
          <Group direction="row" spacing={4} alignItems="center">
            <Icon iconSize={16} icon="th" color={isHighlighted ? Colors.WHITE : Colors.GRAY3} />
            <div>{text}</div>
          </Group>
        );
      case SuggestionType.VALUE:
        const [_, value] = text.split(':');
        return (
          <Group direction="row" spacing={4} alignItems="center">
            <Icon
              iconSize={16}
              icon="filter-list"
              color={isHighlighted ? Colors.WHITE : Colors.GRAY3}
            />
            <div>Filter tag:</div>
            <MenuTag minimal isHighlighted={isHighlighted}>
              {value}
            </MenuTag>
          </Group>
        );
      case SuggestionType.FILTER:
        return (
          <Group direction="row" spacing={4} alignItems="center">
            <Icon
              iconSize={16}
              icon="filter-list"
              color={isHighlighted ? Colors.WHITE : Colors.GRAY3}
            />
            <div>Filter text: &quot;{text}&quot;</div>
          </Group>
        );
    }
  };

  return (
    <Item
      onMouseDown={(e: React.MouseEvent<any>) => {
        e.preventDefault();
        e.stopPropagation();
        onSelect(suggestion);
      }}
      isHighlighted={isHighlighted}
    >
      {_content()}
    </Item>
  );
};

const InputGroup = styled(({hasSuggestions, ...props}) => <BlueprintInputGroup {...props} />)`
  input,
  input:focus {
    outline: none;
    box-shadow: none;
    border: 1px solid ${Colors.LIGHT_GRAY1};
    margin-left: 4px;
    min-width: 600px;
  }

  input:focus {
    border-bottom-left-radius: ${({hasSuggestions}) => (hasSuggestions ? 0 : 3)};
    border-bottom-right-radius: ${({hasSuggestions}) => (hasSuggestions ? 0 : 3)};
  }
`;

const Menu = styled.ul`
  list-style: none;
  margin: 0;
  max-height: 200px;
  max-width: 800px;
  min-width: 602px;
  overflow-y: auto;
  padding: 0;
`;
const Item = styled.li<{
  readonly isHighlighted: boolean;
}>`
  align-items: center;
  background-color: ${({isHighlighted}) => (isHighlighted ? Colors.BLUE3 : Colors.WHITE)};
  color: ${({isHighlighted}) => (isHighlighted ? Colors.WHITE : 'default')};
  cursor: pointer;
  display: flex;
  flex-direction: row;
  font-size: 12px;
  list-style: none;
  margin: 0;
  padding: 4px 8px;
  white-space: nowrap;
  text-overflow: ellipsis;

  &:hover {
    background-color: ${({isHighlighted}) => (isHighlighted ? Colors.BLUE3 : Colors.LIGHT_GRAY3)};
  }
`;

const MenuTag = styled(({isHighlighted, ...rest}) => <BlueprintTag {...rest} />)`
  padding: 1px 5px !important;
  margin: 1px 2px !important;
  overflow: hidden;
  background-color: ${({isHighlighted}) =>
    isHighlighted ? Colors.LIGHT_GRAY1 : Colors.LIGHT_GRAY3} !important;
  max-width: 400px;
`;

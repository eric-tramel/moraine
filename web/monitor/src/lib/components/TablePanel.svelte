<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import type { TableDetailResponse } from '../types/api';
  import type { TableOption } from '../types/ui';
  import { isHttpUrl, previewTableCellValue, searchQueryUrl } from '../utils/url';

  export let tableTitle = 'Table';
  export let tableOptions: TableOption[] = [];
  export let selectedTable: string | null = null;
  export let rowLimit = 25;
  export let rowLimitOptions: number[] = [10, 25, 50, 100];
  export let schemaText = 'No schema metadata returned';
  export let schemaMuted = true;
  export let detail: TableDetailResponse | null = null;

  let selectedTableValue = '';
  let rowLimitValue = 25;
  let expandedCells = new Set<string>();
  let previousDetail: TableDetailResponse | null = null;

  const dispatch = createEventDispatcher<{
    tableChange: string;
    rowLimitChange: number;
  }>();

  $: columns = detail?.schema?.map((column) => column.name) ?? [];
  $: rows = detail?.rows ?? [];
  $: selectedTableValue = selectedTable || '';
  $: rowLimitValue = rowLimit;
  $: if (detail !== previousDetail) {
    previousDetail = detail;
    expandedCells = new Set();
  }

  function handleTableChange(): void {
    dispatch('tableChange', selectedTableValue);
  }

  function handleRowLimitChange(): void {
    dispatch('rowLimitChange', rowLimitValue);
  }

  function valueAsString(value: unknown): string {
    return typeof value === 'string' ? value : String(value);
  }

  function cellKey(rowIndex: number, column: string): string {
    return `${rowIndex}:${column}`;
  }

  function isCellExpanded(key: string): boolean {
    return expandedCells.has(key);
  }

  function toggleCellExpansion(key: string): void {
    const next = new Set(expandedCells);
    if (next.has(key)) {
      next.delete(key);
    } else {
      next.add(key);
    }
    expandedCells = next;
  }
</script>

<section class="panel">
  <section class="detail">
    <div class="detail-head">
      <h2 id="tableTitle">{tableTitle}</h2>

      <label for="tableSelect">Table</label>
      <select id="tableSelect" bind:value={selectedTableValue} on:change={handleTableChange}>
        {#each tableOptions as option}
          <option value={option.value}>{option.label}</option>
        {/each}
      </select>

      <label for="rowLimit">Rows</label>
      <select id="rowLimit" bind:value={rowLimitValue} on:change={handleRowLimitChange}>
        {#each rowLimitOptions as option}
          <option value={option}>{option}</option>
        {/each}
      </select>
    </div>

    <div id="schemaCard" class="card" class:muted={schemaMuted}>{schemaText}</div>

    <div class="table-wrap">
      <table id="previewTable">
        <thead id="previewHead">
          {#if columns.length === 0}
            <tr>
              <th>No schema available</th>
            </tr>
          {:else}
            <tr>
              {#each columns as column}
                <th>{column}</th>
              {/each}
            </tr>
          {/if}
        </thead>

        <tbody id="previewBody">
          {#if columns.length > 0 && rows.length === 0}
            <tr>
              <td colspan={columns.length}>No rows found for this table.</td>
            </tr>
          {:else}
            {#each rows as row, rowIndex}
              <tr>
                {#each columns as column}
                  {@const value = row[column]}
                  <td>
                    {#if value === null || value === undefined}
                      {''}
                    {:else}
                      {@const preview = previewTableCellValue(value)}
                      {@const key = cellKey(rowIndex, column)}
                      {@const expanded = isCellExpanded(key)}
                      {@const displayText = expanded ? preview.fullText : preview.previewText}
                      <div class="cell-content">
                        {#if selectedTable === 'web_searches' && column === 'result_url' && isHttpUrl(value)}
                          <a href={value} target="_blank" rel="noopener noreferrer">{displayText}</a>
                        {:else if selectedTable === 'web_searches' && column === 'search_query' && valueAsString(value).trim().length > 0}
                          <a href={searchQueryUrl(valueAsString(value))} target="_blank" rel="noopener noreferrer">
                            {displayText}
                          </a>
                        {:else}
                          {displayText}
                        {/if}

                        {#if preview.isTruncated}
                          <button
                            type="button"
                            class="cell-toggle"
                            aria-expanded={expanded}
                            on:click={() => toggleCellExpansion(key)}
                          >
                            {expanded ? 'Show less' : 'Show more'}
                          </button>
                        {/if}
                      </div>
                    {/if}
                  </td>
                {/each}
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </section>
</section>

export function isHttpUrl(value: unknown): value is string {
  return typeof value === 'string' && /^https?:\/\//i.test(value);
}

export function searchQueryUrl(query: string): string {
  return `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
}

export const TABLE_CELL_PREVIEW_MAX_CHARS = 240;

export interface TableCellPreviewText {
  fullText: string;
  previewText: string;
  isTruncated: boolean;
}

export function stringifyCellValue(value: unknown): string {
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }
  return String(value);
}

export function previewTableCellValue(
  value: unknown,
  maxChars = TABLE_CELL_PREVIEW_MAX_CHARS,
): TableCellPreviewText {
  const fullText = stringifyCellValue(value);
  const safeMaxChars = Math.max(1, Math.floor(maxChars));

  if (fullText.length <= safeMaxChars) {
    return {
      fullText,
      previewText: fullText,
      isTruncated: false,
    };
  }

  return {
    fullText,
    previewText: `${fullText.slice(0, safeMaxChars).trimEnd()}...`,
    isTruncated: true,
  };
}

import { describe, expect, it } from 'vitest';
import {
  TABLE_CELL_PREVIEW_MAX_CHARS,
  isHttpUrl,
  previewTableCellValue,
  searchQueryUrl,
  stringifyCellValue,
} from './url';

describe('url utils', () => {
  it('detects http and https URLs', () => {
    expect(isHttpUrl('http://example.com')).toBe(true);
    expect(isHttpUrl('https://example.com')).toBe(true);
    expect(isHttpUrl('ftp://example.com')).toBe(false);
    expect(isHttpUrl(42)).toBe(false);
  });

  it('builds duckduckgo query URLs', () => {
    expect(searchQueryUrl('bun typescript')).toBe('https://duckduckgo.com/?q=bun%20typescript');
  });

  it('stringifies primitive and object cell values', () => {
    expect(stringifyCellValue('value')).toBe('value');
    expect(stringifyCellValue(7)).toBe('7');
    expect(stringifyCellValue({ a: 1 })).toBe('{"a":1}');
    expect(stringifyCellValue(null)).toBe('');
  });

  it('returns untruncated previews for short text', () => {
    const preview = previewTableCellValue('short');

    expect(preview).toEqual({
      fullText: 'short',
      previewText: 'short',
      isTruncated: false,
    });
  });

  it('truncates long previews with an ellipsis', () => {
    const longValue = 'x'.repeat(TABLE_CELL_PREVIEW_MAX_CHARS + 20);
    const preview = previewTableCellValue(longValue);

    expect(preview.isTruncated).toBe(true);
    expect(preview.fullText).toBe(longValue);
    expect(preview.previewText.endsWith('...')).toBe(true);
    expect(preview.previewText.length).toBe(TABLE_CELL_PREVIEW_MAX_CHARS + 3);
  });
});

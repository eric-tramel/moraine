import type { Session, SessionsResponse } from '../types/sessions';
import { generateMockSessions } from './sessionsMock';

export interface FetchSessionsOptions {
  allowMock?: boolean;
}

export async function fetchSessions(options: FetchSessionsOptions = {}): Promise<Session[]> {
  const { allowMock = true } = options;

  try {
    const response = await fetch('/api/sessions', {
      headers: { Accept: 'application/json' },
    });

    if (response.ok) {
      const data = (await response.json()) as SessionsResponse;
      if (data.ok && Array.isArray(data.sessions)) {
        return data.sessions;
      }
      if (data.error) {
        throw new Error(data.error);
      }
    } else if (response.status !== 404) {
      throw new Error(`sessions request failed (${response.status})`);
    }
  } catch (error) {
    if (!allowMock) {
      throw error;
    }
  }

  return generateMockSessions();
}

export async function fetchSessionDetail(id: string): Promise<Session | null> {
  try {
    const response = await fetch(`/api/sessions/${encodeURIComponent(id)}`, {
      headers: { Accept: 'application/json' },
    });
    if (!response.ok) {
      return null;
    }
    const data = (await response.json()) as { ok: boolean; session?: Session; error?: string };
    if (data.ok && data.session) {
      return data.session;
    }
  } catch {
    // fall through
  }
  return null;
}

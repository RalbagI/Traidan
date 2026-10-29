export const DEFAULT_ADMIN_API_URL = 'http://127.0.0.1:8090';
export const MIN_OPERATOR_TOKEN_LENGTH = 32;

export class AdminApiClient {
  constructor({
    baseUrl = DEFAULT_ADMIN_API_URL,
    token = '',
    fetchImpl = globalThis.fetch,
  } = {}) {
    this.baseUrl = String(baseUrl || DEFAULT_ADMIN_API_URL).replace(/\/+$/, '');
    this.token = String(token).trim();
    this.fetchImpl = fetchImpl;
  }

  async status() {
    return this.#request('/admin/status');
  }

  async #request(path) {
    if (this.token.length < MIN_OPERATOR_TOKEN_LENGTH) {
      return {
        status: 'failed',
        reason: 'token_required',
        httpStatus: 0,
      };
    }
    if (typeof this.fetchImpl !== 'function') {
      return {
        status: 'failed',
        reason: 'fetch_unavailable',
        httpStatus: 0,
      };
    }
    try {
      const response = await this.fetchImpl.call(globalThis, `${this.baseUrl}${path}`, {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${this.token}`,
        },
      });
      const body = await readJsonBody(response);
      return {
        ...body,
        status: body.status ?? (response.ok ? 'ok' : 'failed'),
        httpStatus: response.status,
      };
    } catch (error) {
      return {
        status: 'failed',
        reason: 'network_error',
        message: error instanceof Error ? error.message : String(error),
        httpStatus: 0,
      };
    }
  }
}

async function readJsonBody(response) {
  const text = await response.text();
  if (!text.trim()) return {};
  try {
    return JSON.parse(text);
  } catch {
    return {
      status: 'failed',
      reason: 'invalid_json',
      raw: text.slice(0, 200),
    };
  }
}

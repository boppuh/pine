import type { ZodType } from "zod";

import {
  apiErrorResponseSchema,
  backendDescriptorSchema,
  captureResponseSchema,
  DEFAULT_SCHEMA_ID,
  extractionResultSchema,
  healthResponseSchema,
  type CaptureResponse,
  type ExtractionResult,
  type HealthResponse,
  preregisteredCaptureRequestSchema,
  type PreregisteredCaptureRequest,
} from "./contracts";

export const BACKEND_DISCOVERY_REF = ".ledger/backend.json";
const MINIMUM_TOKEN_LENGTH = 32;

export interface VaultReader {
  read(path: string): Promise<string>;
}

export interface HTTPRequest {
  url: string;
  method: "GET" | "POST";
  headers: Readonly<Record<string, string>>;
  body?: string;
}

export interface HTTPResponse {
  status: number;
  json: unknown;
}

export interface HTTPTransport {
  request(request: HTTPRequest): Promise<HTTPResponse>;
}

export interface Timer {
  setTimeout(callback: () => void, milliseconds: number): number;
  clearTimeout(handle: number): void;
}

const browserTimer: Timer = {
  setTimeout: (callback, milliseconds) => window.setTimeout(callback, milliseconds),
  clearTimeout: (handle) => window.clearTimeout(handle),
};

export class LedgerClientError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status?: number,
    readonly details: readonly string[] = [],
  ) {
    super(message);
    this.name = "LedgerClientError";
  }
}

export class LedgerBackendClient {
  constructor(
    private readonly vault: VaultReader,
    private readonly transport: HTTPTransport,
    private readonly timeoutMilliseconds = 15_000,
    private readonly timer: Timer = browserTimer,
  ) {
    if (!Number.isInteger(timeoutMilliseconds) || timeoutMilliseconds <= 0) {
      throw new TypeError("timeoutMilliseconds must be a positive integer");
    }
  }

  async health(): Promise<HealthResponse> {
    const connection = await this.discover();
    return this.request(
      connection,
      "/health",
      "GET",
      undefined,
      healthResponseSchema,
      false,
    );
  }

  async createDraft(
    text: string,
    schemaId = DEFAULT_SCHEMA_ID,
  ): Promise<ExtractionResult> {
    const connection = await this.discover();
    return this.request(
      connection,
      "/v1/drafts",
      "POST",
      { text, schema_id: schemaId },
      extractionResultSchema,
      true,
    );
  }

  async capture(request: PreregisteredCaptureRequest): Promise<CaptureResponse> {
    const validated = preregisteredCaptureRequestSchema.parse(request);
    const connection = await this.discover();
    return this.request(
      connection,
      "/v1/captures",
      "POST",
      validated,
      captureResponseSchema,
      true,
    );
  }

  private async discover(): Promise<ConnectionDetails> {
    let descriptorText: string;
    try {
      descriptorText = await this.vault.read(BACKEND_DISCOVERY_REF);
    } catch {
      throw new LedgerClientError(
        "The local ledger backend is not running for this vault.",
        "backend_unavailable",
      );
    }

    let descriptorJSON: unknown;
    try {
      descriptorJSON = JSON.parse(descriptorText);
    } catch {
      throw new LedgerClientError(
        "The backend discovery document is malformed.",
        "invalid_discovery",
      );
    }
    const descriptorResult = backendDescriptorSchema.safeParse(descriptorJSON);
    if (!descriptorResult.success) {
      throw new LedgerClientError(
        "The backend discovery document failed validation.",
        "invalid_discovery",
      );
    }

    let token: string;
    try {
      token = await this.vault.read(descriptorResult.data.token_ref);
    } catch {
      throw new LedgerClientError(
        "The local backend token is unavailable.",
        "token_unavailable",
      );
    }
    if (!isValidToken(token)) {
      throw new LedgerClientError("The local backend token is malformed.", "invalid_token");
    }
    return {
      baseURL: `http://127.0.0.1:${descriptorResult.data.port}`,
      token,
    };
  }

  private async request<T>(
    connection: ConnectionDetails,
    path: string,
    method: "GET" | "POST",
    body: unknown,
    responseSchema: ZodType<T>,
    authenticated: boolean,
  ): Promise<T> {
    const headers: Record<string, string> = { Accept: "application/json" };
    if (authenticated) {
      headers.Authorization = `Bearer ${connection.token}`;
    }
    let encodedBody: string | undefined;
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      encodedBody = JSON.stringify(body);
    }

    let response: HTTPResponse;
    try {
      response = await withTimeout(
        this.transport.request({
          url: `${connection.baseURL}${path}`,
          method,
          headers,
          ...(encodedBody === undefined ? {} : { body: encodedBody }),
        }),
        this.timeoutMilliseconds,
        this.timer,
      );
    } catch (error) {
      if (error instanceof LedgerClientError) {
        throw error;
      }
      throw new LedgerClientError(
        "The local ledger backend could not be reached.",
        "backend_unavailable",
      );
    }

    if (response.status < 200 || response.status >= 300) {
      const apiError = apiErrorResponseSchema.safeParse(response.json);
      if (apiError.success) {
        throw new LedgerClientError(
          apiError.data.error.message,
          apiError.data.error.code,
          response.status,
          apiError.data.error.details,
        );
      }
      throw new LedgerClientError(
        `The local ledger backend returned HTTP ${response.status}.`,
        "invalid_backend_response",
        response.status,
      );
    }

    const parsed = responseSchema.safeParse(response.json);
    if (!parsed.success) {
      throw new LedgerClientError(
        "The local ledger backend returned an invalid response.",
        "invalid_backend_response",
        response.status,
      );
    }
    return parsed.data;
  }
}

interface ConnectionDetails {
  baseURL: string;
  token: string;
}

function isValidToken(token: string): boolean {
  return (
    token.length >= MINIMUM_TOKEN_LENGTH &&
    token.trim() === token &&
    /^[\x21-\x7E]+$/.test(token)
  );
}

function withTimeout<T>(
  promise: Promise<T>,
  timeoutMilliseconds: number,
  timer: Timer,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = timer.setTimeout(() => {
      reject(
        new LedgerClientError(
          "The local ledger backend request timed out. Retry confirmation with the same key.",
          "backend_timeout",
        ),
      );
    }, timeoutMilliseconds);
    promise.then(
      (value) => {
        timer.clearTimeout(timeout);
        resolve(value);
      },
      (error: unknown) => {
        timer.clearTimeout(timeout);
        reject(error instanceof Error ? error : new Error("Backend transport failed"));
      },
    );
  });
}

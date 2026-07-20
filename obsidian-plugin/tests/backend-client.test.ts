import { describe, expect, it, vi } from "vitest";

import {
  BACKEND_DISCOVERY_REF,
  LedgerBackendClient,
  LedgerClientError,
  type HTTPRequest,
  type HTTPResponse,
  type HTTPTransport,
  type Timer,
  type VaultReader,
} from "../src/backend-client";
import type { PreregisteredCaptureRequest } from "../src/contracts";
import {
  captureResponse,
  descriptor,
  forecast,
  readyExtraction,
  TOKEN,
} from "./fixtures";

class ManualTimer implements Timer {
  private nextHandle = 1;
  private readonly callbacks = new Map<number, () => void>();

  get pending(): number {
    return this.callbacks.size;
  }

  setTimeout(callback: () => void): number {
    const handle = this.nextHandle++;
    this.callbacks.set(handle, callback);
    return handle;
  }

  clearTimeout(handle: number): void {
    this.callbacks.delete(handle);
  }

  fireAll(): void {
    const callbacks = [...this.callbacks.values()];
    this.callbacks.clear();
    for (const callback of callbacks) {
      callback();
    }
  }
}

function vaultWith(
  descriptorValue: unknown = descriptor,
  token = TOKEN,
): { vault: VaultReader; read: ReturnType<typeof vi.fn> } {
  const read = vi.fn<(path: string) => Promise<string>>(async (path) => {
    if (path === BACKEND_DISCOVERY_REF) {
      return JSON.stringify(descriptorValue);
    }
    if (path === descriptor.token_ref) {
      return token;
    }
    throw new Error("missing file");
  });
  return { vault: { read }, read };
}

function transportReturning(response: HTTPResponse): {
  transport: HTTPTransport;
  request: ReturnType<typeof vi.fn>;
} {
  const request = vi.fn<(_request: HTTPRequest) => Promise<HTTPResponse>>(
    async (_request) => response,
  );
  return { transport: { request }, request };
}

describe("LedgerBackendClient", () => {
  it("discovers the loopback backend and sends an authenticated draft request", async () => {
    const { vault, read } = vaultWith();
    const { transport, request } = transportReturning({
      status: 200,
      json: readyExtraction,
    });
    const timer = new ManualTimer();
    const client = new LedgerBackendClient(vault, transport, 1_000, timer);

    const result = await client.createDraft("A complete private note.");

    expect(result).toEqual(readyExtraction);
    expect(read).toHaveBeenNthCalledWith(1, ".ledger/backend.json");
    expect(read).toHaveBeenNthCalledWith(2, ".ledger/backend.token");
    expect(request).toHaveBeenCalledOnce();
    const sent = request.mock.calls[0]?.[0] as HTTPRequest;
    expect(sent.url).toBe("http://127.0.0.1:41275/v1/drafts");
    expect(sent.headers).toEqual({
      Accept: "application/json",
      Authorization: `Bearer ${TOKEN}`,
      "Content-Type": "application/json",
    });
    expect(JSON.parse(sent.body ?? "null")).toEqual({
      text: "A complete private note.",
      schema_id: "finance/strategy-edge:1",
    });
    expect(timer.pending).toBe(0);
  });

  it.each([
    [{ ...descriptor, host: "localhost" }, "invalid_discovery"],
    [{ ...descriptor, token_ref: "../../secret" }, "invalid_discovery"],
    [{ ...descriptor, port: 0 }, "invalid_discovery"],
  ])("rejects unsafe discovery before any network request", async (unsafe, code) => {
    const { vault } = vaultWith(unsafe);
    const request = vi.fn(async () => ({ status: 200, json: readyExtraction }));
    const client = new LedgerBackendClient(
      vault,
      { request },
      1_000,
      new ManualTimer(),
    );

    await expect(client.createDraft("note")).rejects.toMatchObject({ code });
    expect(request).not.toHaveBeenCalled();
  });

  it.each(["short", `${TOKEN}\n`, "token with spaces but long enough to be invalid"])(
    "rejects malformed bearer token %j",
    async (token) => {
      const { vault } = vaultWith(descriptor, token);
      const request = vi.fn(async () => ({ status: 200, json: readyExtraction }));
      const client = new LedgerBackendClient(
        vault,
        { request },
        1_000,
        new ManualTimer(),
      );

      await expect(client.createDraft("note")).rejects.toMatchObject({
        code: "invalid_token",
      });
      expect(request).not.toHaveBeenCalled();
    },
  );

  it("returns the backend's stable domain error without leaking the token", async () => {
    const { vault } = vaultWith();
    const { transport } = transportReturning({
      status: 409,
      json: {
        error: {
          code: "fresh_window_conflict",
          message: "the OOS window was already touched",
          details: ["choose a fresh window"],
        },
      },
    });
    const client = new LedgerBackendClient(
      vault,
      transport,
      1_000,
      new ManualTimer(),
    );

    const error = await client.createDraft("note").catch((reason: unknown) => reason);

    expect(error).toBeInstanceOf(LedgerClientError);
    expect(error).toMatchObject({
      code: "fresh_window_conflict",
      status: 409,
      details: ["choose a fresh window"],
    });
    expect(String(error)).not.toContain(TOKEN);
  });

  it("fails closed on a malformed success response", async () => {
    const { vault } = vaultWith();
    const { transport } = transportReturning({
      status: 200,
      json: { status: "ready", proposal: { registration_status: "exploratory" } },
    });
    const client = new LedgerBackendClient(
      vault,
      transport,
      1_000,
      new ManualTimer(),
    );

    await expect(client.createDraft("note")).rejects.toMatchObject({
      code: "invalid_backend_response",
    });
  });

  it("times out without changing or exposing the request", async () => {
    const { vault } = vaultWith();
    const request = vi.fn(
      async (_request: HTTPRequest) =>
        new Promise<HTTPResponse>(() => {
          // Deliberately unresolved transport request.
        }),
    );
    const timer = new ManualTimer();
    const client = new LedgerBackendClient(vault, { request }, 10, timer);

    const pending = client.createDraft("private note");
    await vi.waitFor(() => expect(timer.pending).toBe(1));
    timer.fireAll();

    await expect(pending).rejects.toMatchObject({ code: "backend_timeout" });
    expect(request).toHaveBeenCalledOnce();
  });

  it("validates the capture request and response at the wire boundary", async () => {
    const { vault } = vaultWith();
    const { transport, request } = transportReturning({
      status: 200,
      json: captureResponse,
    });
    const client = new LedgerBackendClient(
      vault,
      transport,
      1_000,
      new ManualTimer(),
    );
    const captureRequest: PreregisteredCaptureRequest = {
      idempotency_key: "obsidian-test-key",
      schema_id: "finance/strategy-edge:1",
      forecast,
      decision: "Run the frozen strategy.",
      lineage: { family_id: "vwap-family" },
      body: "Original note",
    };

    const result = await client.capture(captureRequest);

    expect(result).toEqual(captureResponse);
    const sent = request.mock.calls[0]?.[0] as HTTPRequest;
    expect(sent.url).toBe("http://127.0.0.1:41275/v1/captures");
    expect(JSON.parse(sent.body ?? "null")).toEqual(captureRequest);
  });
});

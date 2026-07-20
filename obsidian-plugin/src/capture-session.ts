import type { LedgerBackendClient } from "./backend-client";
import {
  preregisteredCaptureRequestSchema,
  proposalEditsSchema,
  type CaptureResponse,
  type DraftProposal,
  type ExtractionResult,
  type PreregisteredCaptureRequest,
  type ProposalEdits,
} from "./contracts";

export type PrepareResult =
  | { status: "ready"; session: CaptureSession }
  | { status: "unable"; errors: readonly string[] };

export type IdempotencyKeyFactory = () => string;

export class CaptureWorkflow {
  constructor(
    private readonly client: Pick<LedgerBackendClient, "createDraft" | "capture">,
    private readonly createIdempotencyKey: IdempotencyKeyFactory = defaultIdempotencyKey,
  ) {}

  async prepare(noteBody: string): Promise<PrepareResult> {
    const result: ExtractionResult = await this.client.createDraft(noteBody);
    if (result.status === "unable") {
      return { status: "unable", errors: result.errors };
    }
    const idempotencyKey = this.createIdempotencyKey();
    return {
      status: "ready",
      session: new CaptureSession(
        this.client,
        result.proposal,
        noteBody,
        idempotencyKey,
      ),
    };
  }
}

export class CaptureSession {
  private pendingRequest: PreregisteredCaptureRequest | null = null;

  constructor(
    private readonly client: Pick<LedgerBackendClient, "capture">,
    readonly proposal: DraftProposal,
    private readonly noteBody: string,
    readonly idempotencyKey: string,
  ) {}

  get hasSubmittedRequest(): boolean {
    return this.pendingRequest !== null;
  }

  async confirm(edits: ProposalEdits): Promise<CaptureResponse> {
    if (this.pendingRequest === null) {
      const validatedEdits = proposalEditsSchema.parse(edits);
      this.pendingRequest = preregisteredCaptureRequestSchema.parse({
        idempotency_key: this.idempotencyKey,
        schema_id: this.proposal.schema_id,
        forecast: validatedEdits.forecast,
        decision: validatedEdits.decision,
        lineage: {
          ...this.proposal.lineage,
          family_id: validatedEdits.family_id,
        },
        body: this.noteBody,
      });
    }
    return this.client.capture(this.pendingRequest);
  }
}

function defaultIdempotencyKey(): string {
  return `obsidian-${window.crypto.randomUUID()}`;
}

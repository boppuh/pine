import {
  MarkdownView,
  Notice,
  Plugin,
  requestUrl,
  type RequestUrlParam,
  type TFile,
} from "obsidian";

import {
  LedgerBackendClient,
  LedgerClientError,
  type HTTPRequest,
  type HTTPTransport,
  type VaultReader,
} from "./backend-client";
import { CaptureWorkflow } from "./capture-session";
import {
  HypothesisConfirmationModal,
  type PluginStatus,
} from "./confirmation-modal";

export default class DecisionEdgeLedgerPlugin extends Plugin {
  private statusElement: HTMLElement | null = null;
  private workflow: CaptureWorkflow | null = null;
  private commandRunning = false;
  private modalOpen = false;
  private currentStatus: PluginStatus = "idle";

  override onload(): void {
    this.statusElement = this.addStatusBarItem();
    this.setStatus("idle");

    const vaultReader: VaultReader = {
      read: (path) => this.app.vault.adapter.read(path),
    };
    const client = new LedgerBackendClient(vaultReader, obsidianTransport());
    this.workflow = new CaptureWorkflow(client);

    this.addCommand({
      id: "log-strategy-hypothesis",
      name: "Log strategy hypothesis",
      checkCallback: (checking) => {
        const view = this.app.workspace.getActiveViewOfType(MarkdownView);
        const file = view?.file;
        if (file === null || file === undefined || file.extension !== "md") {
          return false;
        }
        const available = !this.commandRunning && !this.modalOpen;
        if (!checking && available) {
          void this.logHypothesis(file);
        }
        return available;
      },
    });
  }

  override onunload(): void {
    this.workflow = null;
    this.statusElement = null;
  }

  private async logHypothesis(file: TFile): Promise<void> {
    const workflow = this.workflow;
    if (workflow === null || this.commandRunning) {
      return;
    }
    this.commandRunning = true;
    let openedModal = false;
    this.setStatus("extracting");
    try {
      const noteBody = await this.app.vault.cachedRead(file);
      if (noteBody.trim().length === 0) {
        new Notice("The active note is empty; add a complete strategy hypothesis first.");
        this.setStatus("idle");
        return;
      }
      const prepared = await workflow.prepare(noteBody);
      if (prepared.status === "unable") {
        new Notice(
          `Unable to extract a complete hypothesis:\n${prepared.errors.join("\n")}`,
          10_000,
        );
        this.setStatus("error");
        return;
      }
      const modal = new HypothesisConfirmationModal(
        this.app,
        prepared.session,
        (status) => {
          this.setStatus(status);
        },
        () => {
          this.modalOpen = false;
        },
      );
      this.modalOpen = true;
      try {
        modal.open();
        openedModal = true;
      } catch (error) {
        this.modalOpen = false;
        throw error;
      }
    } catch (error) {
      new Notice(commandErrorMessage(error), 10_000);
      this.setStatus("error");
    } finally {
      this.commandRunning = false;
      if (!openedModal && this.currentStatus === "extracting") {
        this.setStatus("idle");
      }
    }
  }

  private setStatus(status: PluginStatus): void {
    this.currentStatus = status;
    this.statusElement?.setText(`Ledger: ${status}`);
  }
}

function obsidianTransport(): HTTPTransport {
  return {
    async request(request: HTTPRequest) {
      const parameters: RequestUrlParam = {
        url: request.url,
        method: request.method,
        headers: { ...request.headers },
        throw: false,
        ...(request.body === undefined
          ? {}
          : { body: request.body, contentType: "application/json" }),
      };
      const response = await requestUrl(parameters);
      return { status: response.status, json: response.json as unknown };
    },
  };
}

function commandErrorMessage(error: unknown): string {
  if (error instanceof LedgerClientError) {
    const details = error.details.length > 0 ? `\n${error.details.join("\n")}` : "";
    return `${error.message}${details}`;
  }
  return "The strategy hypothesis could not be prepared. No ledger record was written.";
}

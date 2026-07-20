import { App, ButtonComponent, Modal, Notice, Setting } from "obsidian";

import type { CaptureSession } from "./capture-session";
import type { ProposalEdits } from "./contracts";
import {
  createProposalPresentation,
  formatCaptureError,
  type ProposalPresentation,
} from "./presentation";

export type PluginStatus =
  | "idle"
  | "extracting"
  | "reviewing"
  | "capturing"
  | "captured"
  | "error";

export type StatusReporter = (status: PluginStatus) => void;

export class HypothesisConfirmationModal extends Modal {
  private readonly edits: ProposalEdits;
  private errorElement: HTMLElement | null = null;
  private confirmButton: ButtonComponent | null = null;
  private capturing = false;
  private succeeded = false;
  private readonly presentation: ProposalPresentation;

  constructor(
    app: App,
    private readonly session: CaptureSession,
    private readonly reportStatus: StatusReporter,
    private readonly reportClosed: () => void,
  ) {
    super(app);
    this.presentation = createProposalPresentation(session.proposal);
    this.edits = this.presentation.edits;
  }

  override onOpen(): void {
    this.modalEl.addClass("decision-edge-modal");
    this.setTitle("Confirm strategy hypothesis");
    this.renderSummary();
    this.renderForecast();
    this.renderWindows();
    this.renderReasoning();
    this.renderActions();
    this.reportStatus("reviewing");
  }

  override onClose(): void {
    this.contentEl.empty();
    this.reportClosed();
    if (!this.succeeded) {
      this.reportStatus("idle");
    }
  }

  override close(): void {
    if (this.capturing && !this.succeeded) {
      return;
    }
    super.close();
  }

  private renderSummary(): void {
    this.contentEl.createEl("p", {
      cls: "decision-edge-summary",
      text: this.presentation.summary,
    });
    if (this.presentation.warning === null) {
      this.contentEl.createEl("p", {
        cls: "decision-edge-summary",
        text: this.presentation.advisory,
      });
    } else {
      this.contentEl.createDiv({
        cls: "decision-edge-warning",
        text: this.presentation.warning,
      });
    }
  }

  private renderForecast(): void {
    this.contentEl.createEl("h3", { text: "Forecast" });
    addTextSetting(
      this.contentEl,
      "Strategy ID",
      this.edits.forecast.strategy_id,
      (value) => {
        this.edits.forecast.strategy_id = value;
      },
    );
    addNumberSetting(
      this.contentEl,
      "Expected Sharpe",
      this.edits.forecast.expected_metrics.sharpe,
      (value) => {
        this.edits.forecast.expected_metrics.sharpe = value;
      },
    );
    addNumberSetting(
      this.contentEl,
      "Expected win rate",
      this.edits.forecast.expected_metrics.win_rate,
      (value) => {
        this.edits.forecast.expected_metrics.win_rate = value;
      },
      { min: 0, max: 1, step: 0.01 },
    );
    addNumberSetting(
      this.contentEl,
      "Expected max drawdown",
      this.edits.forecast.expected_metrics.max_drawdown,
      (value) => {
        this.edits.forecast.expected_metrics.max_drawdown = value;
      },
    );
    addNumberSetting(
      this.contentEl,
      "Expected expectancy",
      this.edits.forecast.expected_metrics.expectancy,
      (value) => {
        this.edits.forecast.expected_metrics.expectancy = value;
      },
    );
  }

  private renderWindows(): void {
    this.contentEl.createEl("h3", { text: "Research Windows" });
    addDateSetting(
      this.contentEl,
      "In-sample start",
      this.edits.forecast.in_sample_window.start,
      (value) => {
        this.edits.forecast.in_sample_window.start = value;
      },
    );
    addDateSetting(
      this.contentEl,
      "In-sample end",
      this.edits.forecast.in_sample_window.end,
      (value) => {
        this.edits.forecast.in_sample_window.end = value;
      },
    );
    addDateSetting(
      this.contentEl,
      "Out-of-sample start",
      this.edits.forecast.out_of_sample_window.start,
      (value) => {
        this.edits.forecast.out_of_sample_window.start = value;
      },
    );
    addDateSetting(
      this.contentEl,
      "Out-of-sample end",
      this.edits.forecast.out_of_sample_window.end,
      (value) => {
        this.edits.forecast.out_of_sample_window.end = value;
      },
    );
  }

  private renderReasoning(): void {
    this.contentEl.createEl("h3", { text: "Decision and attribution" });
    addTextAreaSetting(
      this.contentEl,
      "Decision",
      this.edits.decision,
      (value) => {
        this.edits.decision = value;
      },
    );
    addTextAreaSetting(
      this.contentEl,
      "Invalidation",
      this.edits.forecast.invalidation,
      (value) => {
        this.edits.forecast.invalidation = value;
      },
    );
    addTextAreaSetting(
      this.contentEl,
      "Edge source",
      this.edits.forecast.edge_source,
      (value) => {
        this.edits.forecast.edge_source = value;
      },
    );
    addTextSetting(this.contentEl, "Family ID", this.edits.family_id, (value) => {
      this.edits.family_id = value;
    });
  }

  private renderActions(): void {
    const actionSetting = new Setting(this.contentEl).setClass("decision-edge-actions");
    actionSetting.addButton((button) => {
      button.setButtonText("Cancel").onClick(() => {
        if (!this.capturing) {
          this.close();
        }
      });
    });
    actionSetting.addButton((button) => {
      this.confirmButton = button;
      button
        .setButtonText("Confirm preregistration")
        .setCta()
        .onClick(() => void this.confirm());
    });
    this.errorElement = this.contentEl.createDiv({
      cls: "decision-edge-error",
      attr: { role: "alert" },
    });
  }

  private async confirm(): Promise<void> {
    if (this.capturing) {
      return;
    }
    this.capturing = true;
    this.setInputsDisabled(true);
    this.confirmButton?.setDisabled(true).setButtonText("Capturing…");
    this.setError("");
    this.reportStatus("capturing");
    try {
      const result = await this.session.confirm(this.edits);
      this.succeeded = true;
      this.reportStatus("captured");
      new Notice(
        result.created
          ? `Preregistered ${result.prediction_id}.`
          : `Preregistration ${result.prediction_id} already exists.`,
        8_000,
      );
      this.close();
    } catch (error) {
      this.reportStatus("error");
      this.setError(formatCaptureError(error));
      this.capturing = false;
      if (this.session.hasSubmittedRequest) {
        this.confirmButton?.setDisabled(false).setButtonText("Retry confirmation");
      } else {
        this.setInputsDisabled(false);
        this.confirmButton?.setDisabled(false).setButtonText("Confirm preregistration");
      }
    }
  }

  private setError(message: string): void {
    if (this.errorElement !== null) {
      this.errorElement.setText(message);
    }
  }

  private setInputsDisabled(disabled: boolean): void {
    for (const input of Array.from(
      this.contentEl.querySelectorAll("input, textarea"),
    )) {
      if (
        input.instanceOf(HTMLInputElement) ||
        input.instanceOf(HTMLTextAreaElement)
      ) {
        input.disabled = disabled;
      }
    }
  }
}

function addTextSetting(
  container: HTMLElement,
  name: string,
  initialValue: string,
  update: (value: string) => void,
): void {
  new Setting(container).setName(name).addText((text) => {
    text.setValue(initialValue).onChange(update);
  });
}

function addTextAreaSetting(
  container: HTMLElement,
  name: string,
  initialValue: string,
  update: (value: string) => void,
): void {
  new Setting(container).setName(name).addTextArea((text) => {
    text.setValue(initialValue).onChange(update);
    text.inputEl.rows = 3;
  });
}

interface NumberOptions {
  min?: number;
  max?: number;
  step?: number;
}

function addNumberSetting(
  container: HTMLElement,
  name: string,
  initialValue: number,
  update: (value: number) => void,
  options: NumberOptions = {},
): void {
  new Setting(container).setName(name).addText((text) => {
    text.inputEl.type = "number";
    text.inputEl.step = String(options.step ?? "any");
    if (options.min !== undefined) {
      text.inputEl.min = String(options.min);
    }
    if (options.max !== undefined) {
      text.inputEl.max = String(options.max);
    }
    text.setValue(String(initialValue)).onChange((value) => {
      update(value.trim() === "" ? Number.NaN : Number(value));
    });
  });
}

function addDateSetting(
  container: HTMLElement,
  name: string,
  initialValue: string,
  update: (value: string) => void,
): void {
  new Setting(container).setName(name).addText((text) => {
    text.inputEl.type = "date";
    text.setValue(initialValue).onChange(update);
  });
}

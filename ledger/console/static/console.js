"use strict";

const source = document.querySelector("[data-source-text]");
const characterCount = document.querySelector("[data-character-count]");
if (source instanceof HTMLTextAreaElement && characterCount instanceof HTMLOutputElement) {
  const updateCount = () => {
    characterCount.value = `${source.value.length.toLocaleString()} / 200,000`;
  };
  source.addEventListener("input", updateCount);
  updateCount();
}

for (const form of document.querySelectorAll("form[data-submit-once]")) {
  form.addEventListener("submit", () => {
    const controls = form.hasAttribute("data-lock-workflow")
      ? document.querySelectorAll(
          "form[data-lock-workflow] button, form[data-lock-workflow] input, " +
            "form[data-lock-workflow] select, form[data-lock-workflow] textarea, " +
            ".secondary-form button",
        )
      : form.querySelectorAll("button, input, select, textarea");
    for (const control of controls) {
      if (control instanceof HTMLButtonElement) {
        const pendingLabel = control.dataset.pendingLabel;
        if (pendingLabel) control.textContent = pendingLabel;
      }
      control.setAttribute("aria-disabled", "true");
      if (control instanceof HTMLButtonElement) control.disabled = true;
      if (control instanceof HTMLInputElement && control.type !== "hidden") {
        control.readOnly = true;
      }
      if (control instanceof HTMLTextAreaElement) control.readOnly = true;
    }
  });
}

const freshness = document.querySelector("[data-freshness]");
if (freshness instanceof HTMLElement) {
  const title = freshness.querySelector("[data-freshness-title]");
  const message = freshness.querySelector("[data-freshness-message]");
  const family = document.querySelector("#family_id");
  const oosStart = document.querySelector("#out_of_sample_start");
  const oosEnd = document.querySelector("#out_of_sample_end");
  const inputs = [family, oosStart, oosEnd];
  const updateFreshness = () => {
    const changed =
      family instanceof HTMLInputElement &&
      oosStart instanceof HTMLInputElement &&
      oosEnd instanceof HTMLInputElement &&
      (family.value !== freshness.dataset.originalFamily ||
        oosStart.value !== freshness.dataset.originalOosStart ||
        oosEnd.value !== freshness.dataset.originalOosEnd);
    if (changed) {
      freshness.classList.add("is-warning");
      if (title) title.textContent = "Freshness changed";
      if (message) {
        message.textContent =
          "Changed since advisory check; Pine will re-check on confirmation.";
      }
    }
  };
  for (const input of inputs) input?.addEventListener("input", updateFreshness);
}

const errorSummary = document.querySelector("[data-error-summary]");
if (errorSummary instanceof HTMLElement) errorSummary.focus();

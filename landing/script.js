const calculator = document.querySelector("[data-calculator]");
const pilotForm = document.querySelector("[data-pilot-form]");

if (calculator) {
  const capacity = calculator.querySelector("[data-capacity]");
  const days = calculator.querySelector("[data-days]");
  const value = calculator.querySelector("[data-value]");
  const capacityOutput = calculator.querySelector("[data-capacity-output]");
  const daysOutput = calculator.querySelector("[data-days-output]");
  const valueOutput = calculator.querySelector("[data-value-output]");
  const result = calculator.querySelector("[data-result]");

  const money = new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 1,
    minimumFractionDigits: 1,
  });

  const compactMoney = (amount) => {
    if (amount >= 1_000_000) return `$${money.format(amount / 1_000_000)}M`;
    return `$${Math.round(amount / 1_000)}k`;
  };

  const update = () => {
    const mw = Number(capacity.value);
    const leadDays = Number(days.value);
    const valuePerMw = Number(value.value);
    const timeFactor = Math.min(1.6, Math.max(0.25, leadDays / 14));
    const modeled = mw * valuePerMw * timeFactor;

    capacityOutput.textContent = `${mw} MW`;
    daysOutput.textContent = `${leadDays} days`;
    valueOutput.textContent = compactMoney(valuePerMw);
    result.textContent = compactMoney(modeled);
  };

  capacity.addEventListener("input", update);
  days.addEventListener("input", update);
  value.addEventListener("input", update);
  update();
}

if (pilotForm) {
  const status = pilotForm.querySelector("[data-form-status]");
  const fallbackEmail = "founders@queuewatch.ai";

  const setStatus = (message, type = "") => {
    status.textContent = message;
    status.className = `form-status ${type}`.trim();
  };

  const fallbackMailto = (payload) => {
    const subject = encodeURIComponent("QueueWatch pilot request");
    const body = encodeURIComponent(
      [
        "We want to pilot QueueWatch.",
        "",
        `Name: ${payload.name || ""}`,
        `Email: ${payload.email || ""}`,
        `Company: ${payload.company || ""}`,
        `Role: ${payload.role || ""}`,
        `Priority territories: ${payload.territories || ""}`,
        `Use case: ${payload.use_case || ""}`,
      ].join("\n"),
    );
    window.location.href = `mailto:${fallbackEmail}?subject=${subject}&body=${body}`;
  };

  pilotForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const formData = new FormData(pilotForm);
    const payload = Object.fromEntries(formData.entries());
    const endpoint = pilotForm.dataset.endpoint;

    if (!endpoint) {
      fallbackMailto(payload);
      return;
    }

    const submitButton = pilotForm.querySelector("button[type='submit']");
    submitButton.disabled = true;
    setStatus("Sending pilot request...");

    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(result.error || "Pilot request failed.");
      }
      pilotForm.reset();
      setStatus("Pilot request received. We will follow up with the next step.", "success");
    } catch (error) {
      setStatus(
        "The form could not submit. Opening an email draft instead.",
        "error",
      );
      fallbackMailto(payload);
    } finally {
      submitButton.disabled = false;
    }
  });
}

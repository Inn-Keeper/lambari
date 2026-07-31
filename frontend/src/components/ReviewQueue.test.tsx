import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ReviewQueue } from "./ReviewQueue";

const counts = { open: 1, confirmed_fraud: 0, false_positive: 0 };
const caseBody = {
  cases: [
    {
      id: "tx_1",
      verdict: {
        tx_id: "tx_1",
        card_bin: "520082",
        amount: 123.45,
        currency: "SEK",
        country: "SE",
        score: 55,
        flags: ["amount_high"],
        decision: "review",
        latency_us: 10,
        at: 0,
      },
      status: "open",
      opened_at: 0,
    },
  ],
};

const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });

function stubFetch(resolveStatus: number) {
  const f = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.endsWith("/resolve")) return new Response("{}", { status: resolveStatus });
    return ok(caseBody);
  });
  vi.stubGlobal("fetch", f);
  return f;
}

afterEach(() => vi.unstubAllGlobals());

describe("ReviewQueue", () => {
  it("loads via the push signal and resolves optimistically", async () => {
    stubFetch(200);
    render(<ReviewQueue counts={counts} tick={1} />);
    await screen.findByText("123.45 SEK");

    await userEvent.click(screen.getByRole("button", { name: /confirm fraud/i }));
    await waitFor(() =>
      expect(screen.queryByText("123.45 SEK")).not.toBeInTheDocument(),
    );
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("rolls back and shows an error when the resolve fails", async () => {
    stubFetch(500);
    render(<ReviewQueue counts={counts} tick={1} />);
    await screen.findByText("123.45 SEK");

    await userEvent.click(screen.getByRole("button", { name: /clear/i }));
    await screen.findByRole("alert");
    expect(screen.getByText("123.45 SEK")).toBeInTheDocument();
    expect(screen.getByRole("alert").textContent).toContain("tx_1");
  });

  it("holds the list still while hovered and applies fresh data on unhover", async () => {
    const second = {
      cases: [{ ...caseBody.cases[0], id: "tx_2", verdict: { ...caseBody.cases[0].verdict, tx_id: "tx_2", amount: 999.99 } }],
    };
    let body: unknown = caseBody;
    vi.stubGlobal("fetch", vi.fn(async () => ok(body)));

    const { rerender } = render(<ReviewQueue counts={counts} tick={1} />);
    await screen.findByText("123.45 SEK");

    await userEvent.hover(screen.getByText(/review queue/i));
    body = second;
    rerender(<ReviewQueue counts={counts} tick={2} />); // next SSE frame while hovered

    await screen.findByRole("status"); // "updates paused · 1 new"
    expect(screen.getByText("123.45 SEK")).toBeInTheDocument(); // held still
    expect(screen.queryByText("999.99 SEK")).not.toBeInTheDocument();

    await userEvent.unhover(screen.getByText(/review queue/i));
    await screen.findByText("999.99 SEK"); // fresh list applied
    // the replaced row exits via animation — wait for it to actually leave
    await waitFor(() =>
      expect(screen.queryByText("123.45 SEK")).not.toBeInTheDocument(),
    );
    expect(screen.queryByRole("status")).not.toBeInTheDocument();
  });

  it("drops the row without rollback when the case is already gone (404)", async () => {
    stubFetch(404);
    render(<ReviewQueue counts={counts} tick={1} />);
    await screen.findByText("123.45 SEK");

    await userEvent.click(screen.getByRole("button", { name: /clear/i }));
    await screen.findByRole("alert");
    expect(screen.queryByText("123.45 SEK")).not.toBeInTheDocument();
    expect(screen.getByRole("alert").textContent).toMatch(/already resolved or evicted/);
  });
});

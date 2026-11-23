import { expect, it, vi } from "vitest";
import { call, distance } from "./api";
it("formats walking distance on both sides of one kilometre", () => {
  expect(distance(140)).toBe("140 m");
  expect(distance(1250)).toBe("1.3 km");
});
it("preserves large order IDs and surfaces server failures", async () => {
  vi.stubGlobal("localStorage", { getItem: () => null });
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ success: true, data: "9007199254740993" }),
      })
      .mockResolvedValueOnce({
        ok: false,
        json: async () => ({ errorMsg: "请先登录" }),
      }),
  );
  expect(await call("/orders")).toBe("9007199254740993");
  await expect(call("/orders")).rejects.toThrow("请先登录");
  vi.unstubAllGlobals();
});

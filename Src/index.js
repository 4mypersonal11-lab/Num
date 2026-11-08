const API_KEYS = new Set([
  "black",
  "sudo"
]);

const NEW_API = "https://tfqdeadlo-1-78bapi.hf.space/search?mobile=";

const CREDIT = {
  name: "black",
  developer: "@heart_of_public",
  contact: "https://t.me/sufiyanr31",
  footer: "Powered by black"
};

export default {
  async fetch(request) {
    const reqUrl = new URL(request.url);

    // Health check
    if (reqUrl.pathname === "/") {
      return Response.json({
        status: true,
        message: "API is running"
      });
    }

    // Only /api route
    if (reqUrl.pathname !== "/api") {
      return Response.json({
        status: false,
        message: "Not Found",
        credit: CREDIT
      }, { status: 404 });
    }

    const key = reqUrl.searchParams.get("key");
    const number = reqUrl.searchParams.get("number");

    // API Key Check
    if (!API_KEYS.has(key)) {
      return Response.json({
        status: false,
        message: "Invalid API Key",
        credit: CREDIT
      }, { status: 401 });
    }

    // Number Check
    if (!number) {
      return Response.json({
        status: false,
        message: "Number is required",
        credit: CREDIT
      }, { status: 400 });
    }

    try {
      const apiUrl =
        `${NEW_API}${encodeURIComponent(number)}`;

      const response = await fetch(apiUrl);

      if (!response.ok) {
        return Response.json({
          status: false,
          message: "Source API unavailable",
          credit: CREDIT
        }, { status: 503 });
      }

      const data = await response.json();

      // Remove upstream promotional fields
      const cleanData = JSON.parse(JSON.stringify(data));

      function removeUpstreamCredits(obj) {
        if (Array.isArray(obj)) {
          return obj.map(removeUpstreamCredits);
        }

        if (obj !== null && typeof obj === "object") {
          const cleaned = {};

          for (const [key, value] of Object.entries(obj)) {
            const normalizedKey = key.toLowerCase();

            if (
              normalizedKey === "api_by" ||
              normalizedKey === "contact" ||
              normalizedKey === "telegram" ||
              normalizedKey === "channel"
            ) {
              continue;
            }

            cleaned[key] = removeUpstreamCredits(value);
          }

          return cleaned;
        }

        if (typeof obj === "string") {
          return obj
            .split("\n")
            .filter(line =>
              !/^\s*(telegram\s*:|channel\s*:)/i.test(line)
            )
            .join("\n")
            .trim();
        }

        return obj;
      }

      const result = removeUpstreamCredits(cleanData);

      // Return cleaned response with ASUR credit
      return Response.json({
        ...(
          result !== null &&
          typeof result === "object" &&
          !Array.isArray(result)
            ? result
            : { result }
        ),
        credit: CREDIT
      }, { status: 200 });

    } catch (e) {
      return Response.json({
        status: false,
        message: "Internal server error",
        credit: CREDIT
      }, { status: 500 });
    }
  }
};

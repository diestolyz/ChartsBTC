module.exports = {
  apps: [
    {
      name: "poly",
      script: "server.js",
      cwd: "/www/wwwroot/polymarket.com",
      env: {
        NODE_ENV: "production",
        HOST: "0.0.0.0",
        PORT: 3004,
      },
    },
  ],
};

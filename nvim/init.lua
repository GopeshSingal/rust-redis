----- Language Server Protocols -----

-- Rust LSP (rust-analyzer)
vim.lsp.config("rust_analyzer", {
  cmd = { "rust-analyzer" }, -- should be on PATH from rustup component
  filetypes = { "rust" },
  settings = {
    ["rust-analyzer"] = {
      cargo = {
        allFeatures = true,
      },
      check = {
        command = "clippy",
      },
      procMacro = {
        enable = true,
      },
    },
  },
})
vim.lsp.enable("rust_analyzer")

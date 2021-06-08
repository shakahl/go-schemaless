{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-21.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = self:
  self.flake-utils.lib.eachDefaultSystem (system:
  let
    pkgs = self.nixpkgs.legacyPackages.${system};
    lib = self.nixpkgs.lib;
  in
  rec {
    packages = self.flake-utils.lib.flattenTree {
      go-schemaless = (import ./.) { inherit pkgs lib;};
    };
    defaultPackage = packages.go-schemaless;
    devShell = (import ./shell.nix) { inherit pkgs lib; };
  }
  );
}

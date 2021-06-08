{ pkgs ? (import <nixpkgs> {}), lib ? (import <nixpkgs/lib>) }:
pkgs.mkShell {
  # nativeBuildInputs is usually what you want -- tools you need to run
  nativeBuildInputs = with pkgs.buildPackages; [ go protobuf ];
}

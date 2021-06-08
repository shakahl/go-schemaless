{ pkgs ? (import <nixpkgs> {}), lib ? (import <nixpkgs/lib>) }:
pkgs.buildGoModule rec {
  name = "go-schemaless";
  src = ./. ;
  vendorSha256 = "83hT7SIZlQ4EPdzA5jN+so59a5yIcBrnBREs4EfpuUc=";
}

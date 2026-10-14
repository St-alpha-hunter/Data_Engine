"""
经本机代理从 Docker Hub 下载镜像，打包成 docker-archive，再 docker load（不用改 Docker 的配置）

用于镜像站（daocloud 等）拉不到的时候。每一层单独下载、已下载的跳过，中断后重跑会接着下。

用法：
    python tools/pull_image_via_proxy.py apache/airflow:3.3.2-python3.10
    python tools/pull_image_via_proxy.py apache/airflow:3.3.2-python3.10 --proxy http://127.0.0.1:7890
"""
import argparse
import hashlib
import json
import subprocess
import tarfile
from pathlib import Path

import requests

REGISTRY = "https://registry-1.docker.io"
WORK_DIR = Path(__file__).resolve().parent.parent / "data" / "image_cache"
ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def token(session: requests.Session, repo: str) -> str:
    r = session.get(f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull", timeout=60)
    r.raise_for_status()
    return r.json()["token"]


def get_manifest(session, repo, ref, headers):
    r = session.get(f"{REGISTRY}/v2/{repo}/manifests/{ref}", headers={**headers, "Accept": ACCEPT}, timeout=60)
    r.raise_for_status()
    return r.json()


def download_blob(session, repo, digest, headers, dest: Path):
    if dest.exists() and "sha256:" + hashlib.sha256(dest.read_bytes()).hexdigest() == digest:
        print(f"  已有 {digest[:19]}")
        return
    tmp = dest.with_suffix(".part")
    with session.get(f"{REGISTRY}/v2/{repo}/blobs/{digest}", headers=headers, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        done = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
                done += len(chunk)
        print(f"  下载 {digest[:19]}  {done / 1e6:.1f} MB" + (f" / {total / 1e6:.1f} MB" if total else ""))
    if "sha256:" + hashlib.sha256(tmp.read_bytes()).hexdigest() != digest:
        raise RuntimeError(f"{digest} 校验失败，重跑会重新下载")
    tmp.replace(dest)


def main():
    parser = argparse.ArgumentParser(description="经代理从 Docker Hub 下载镜像并 docker load")
    parser.add_argument("image", help="如 apache/airflow:3.3.2-python3.10")
    parser.add_argument("--proxy", default="http://127.0.0.1:7890")
    parser.add_argument("--platform", default="linux/amd64")
    args = parser.parse_args()

    name, tag = args.image.rsplit(":", 1)
    repo = name if "/" in name else f"library/{name}"
    session = requests.Session()
    session.proxies = {"https": args.proxy, "http": args.proxy}
    headers = {"Authorization": f"Bearer {token(session, repo)}"}

    manifest = get_manifest(session, repo, tag, headers)
    if "manifests" in manifest:                                   # 多平台：选 linux/amd64
        os_, arch = args.platform.split("/")
        entry = next(m for m in manifest["manifests"]
                     if m.get("platform", {}).get("os") == os_ and m.get("platform", {}).get("architecture") == arch)
        manifest = get_manifest(session, repo, entry["digest"], headers)

    work = WORK_DIR / args.image.replace("/", "_").replace(":", "_")
    work.mkdir(parents=True, exist_ok=True)
    print(f"{args.image}：{len(manifest['layers'])} 层，下载到 {work}")
    config_digest = manifest["config"]["digest"]
    download_blob(session, repo, config_digest, headers, work / f"{config_digest[7:]}.json")
    layer_files = []
    for layer in manifest["layers"]:
        headers = {"Authorization": f"Bearer {token(session, repo)}"}       # token 有效期短，每层重新取
        dest = work / f"{layer['digest'][7:]}.tar.gz"
        download_blob(session, repo, layer["digest"], headers, dest)
        layer_files.append(dest.name)

    # docker-archive 格式：manifest.json + 配置 + 各层（docker load 能直接读压缩的层）
    (work / "manifest.json").write_text(json.dumps([{
        "Config": f"{config_digest[7:]}.json", "RepoTags": [args.image], "Layers": layer_files}]), encoding="utf-8")
    archive = work.parent / f"{work.name}.tar"
    with tarfile.open(archive, "w") as tar:
        for f in ["manifest.json", f"{config_digest[7:]}.json", *layer_files]:
            tar.add(work / f, arcname=f)
    print(f"打包完成：{archive}，开始 docker load")
    subprocess.run(["docker", "load", "-i", str(archive)], check=True)
    print("完成")


if __name__ == "__main__":
    main()

# RUN THIS TO APPLY ALL THE PATCHES TO THE REPO
cd ~/work/TQP-Vortex-Baselines/dpfproto/DPFProto-ramdisk

for p in ../patches/*.patch; do
  if git apply --reverse --check "$p" >/dev/null 2>&1; then
    echo "already applied: $p"
  else
    git apply "$p"
    echo "applied: $p"
  fi
done
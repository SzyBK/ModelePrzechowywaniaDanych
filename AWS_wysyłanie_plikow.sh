#!/usr/bin/env bash
set -euo pipefail

##############################################
## KONFIGURACJA
##############################################

BASE_DIR="/mnt/data"
TESTFILES_DIR="${BASE_DIR}/testfiles"
RESULTS_CSV="${BASE_DIR}/upload_results_aws.csv"

AWS_BUCKET="arch-par-lab-01"
AWS_PREFIXES=("standard" "standard-ia" "glacier")   #
AWS_CLASSES=("STANDARD" "STANDARD_IA" "Glacier")

FILES=(
  "test_20GB.bin:20G"
  "test_50GB.bin:50G"
  "test_100GB.bin:100G"
)

RUNS_PER_FILE=3

##############################################
## FUNKCJE
##############################################
check_deps() {
  local deps=("aws" "stat" "bc" "truncate")
  for d in "${deps[@]}"; do
    command -v "$d" >/dev/null 2>&1 || { echo "ERROR: Brak narzędzia: $d"; exit 1; }
  done
}

now_ns(){ date +%s%N; }
elapsed_seconds(){ echo "scale=6; ($2 - $1) / 1000000000" | bc; }
calc_throughput_mb_s(){
  local size_bytes="$1"; local elapsed_s="$2"
  [[ "$elapsed_s" == "0" ]] && echo "0" || echo "scale=3; ($size_bytes / 1024 / 1024) / $elapsed_s" | bc
}

write_csv_header() {
  if [[ ! -f "$RESULTS_CSV" ]]; then
    echo "provider,storage_tier_or_class,operation,object_path,run_index,bytes,elapsed_s,throughput_MBps" > "$RESULTS_CSV"
  fi
}

append_csv_row() {
  # provider=AWS
  echo "AWS,$1,$2,$3,$4,$5,$6,$7" >> "$RESULTS_CSV"
}

create_test_files() {
  mkdir -p "$TESTFILES_DIR"
  for entry in "${FILES[@]}"; do
    local name="${entry%%:*}"
    local size="${entry##*:}"
    local path="${TESTFILES_DIR}/${name}"
    if [[ ! -f "$path" ]]; then
      echo "Tworzę plik: $path ($size) [sparse]"
      truncate -s "$size" "$path"
    fi
  done
}

delete_s3_object_if_exists() {
  local key="$1"
  aws s3 rm "s3://${AWS_BUCKET}/${key}" >/dev/null 2>&1 || true
}

upload_one() {
  local local_file="$1"
  local prefix="$2"
  local storage_class="$3"
  local run="$4"

  local file_name
  file_name="$(basename "$local_file")"
  local key="${prefix}/${file_name}"
  local uri="s3://${AWS_BUCKET}/${key}"

  # Kasujemy poprzednią wersję obiektu, żeby ten sam plik mógł być wrzucony 3 razy
  delete_s3_object_if_exists "$key"

  local size_bytes start_ns end_ns elapsed_s throughput
  size_bytes=$(stat -c%s "$local_file")
  start_ns=$(now_ns)

  echo "=== [AWS] Upload run ${run}: ${local_file} -> ${uri} (class: ${storage_class}) ==="
  aws s3 cp "$local_file" "$uri" --storage-class "$storage_class"

  end_ns=$(now_ns)
  elapsed_s=$(elapsed_seconds "$start_ns" "$end_ns")
  throughput=$(calc_throughput_mb_s "$size_bytes" "$elapsed_s")

  echo "Czas: ${elapsed_s}s | Rozmiar: ${size_bytes}B | Śr. pasmo: ${throughput} MB/s"
  append_csv_row "$storage_class" "UPLOAD" "$uri" "$run" "$size_bytes" "$elapsed_s" "$throughput"
}

main() {
  check_deps
  write_csv_header
  create_test_files

  echo "INFO: Upewnij się, że AWS CLI ma poświadczenia (IAM role / aws configure)."
  echo

  for ((run=1; run<=RUNS_PER_FILE; run++)); do
    for entry in "${FILES[@]}"; do
      local name="${entry%%:*}"
      local path="${TESTFILES_DIR}/${name}"

      for i in "${!AWS_PREFIXES[@]}"; do
        upload_one "$path" "${AWS_PREFIXES[$i]}" "${AWS_CLASSES[$i]}" "$run"
      done
    done
  done

  echo "Zakończono AWS. Wyniki: $RESULTS_CSV"
}

main "$@"

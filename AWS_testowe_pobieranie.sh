#!/usr/bin/env bash
set -euo pipefail

##############################################
## KONFIGURACJA – DANE
##############################################

# AWS S3
S3_BUCKET_STANDARD="arch-par-lab-01"      # <-- nazwa Bucket

# Opis testowanej klasy S3
S3_STORAGE_CLASS="STANDARD"
#S3_STORAGE_CLASS="STANDARD-AI"
#S3_STORAGE_CLASS="GLACIER"

# Lista obiektów S3 do testu
S3_KEYS=(
  "standard/test_20GB.bin"
  "standard/test_50GB.bin"
  "standard/test_100GB.bin"
)

# Ilu powtórzeń dla każdego pliku
RUNS_PER_FILE=3

# Katalog na pobrane pliki i logi
BASE_DIR="/mnt/data"
DOWNLOAD_DIR="${BASE_DIR}/downloads_aws"

# Plik wynikowy (CSV) – wspólny z Azure (bez kolumny sha256)
RESULTS_CSV="${BASE_DIR}/download_results.csv"

##############################################
## FUNKCJE POMOCNICZE
##############################################

check_deps() {
  # sha256sum usunięte – nie jest już wymagane
  local deps=("aws" "stat" "bc")
  for d in "${deps[@]}"; do
    if ! command -v "$d" >/dev/null 2>&1; then
      echo "ERROR: Brak narzędzia: $d. Zainstaluj je przed uruchomieniem."
      exit 1
    fi
  done
}

now_ns() { date +%s%N; }

elapsed_seconds() {
  local start_ns="$1"
  local end_ns="$2"
  echo "scale=6; ($end_ns - $start_ns) / 1000000000" | bc
}

calc_throughput_mb_s() {
  local size_bytes="$1"
  local elapsed_s="$2"
  if [[ "$elapsed_s" == "0" ]]; then
    echo "0"
  else
    echo "scale=3; ($size_bytes / 1024 / 1024) / $elapsed_s" | bc
  fi
}

write_csv_header() {
  if [[ ! -f "$RESULTS_CSV" ]]; then
    # Bez kolumny sha256
    echo "provider,storage_tier_or_class,file_name,file_logical_path,run_index,bytes,elapsed_s,throughput_MBps" > "$RESULTS_CSV"
  fi
}

append_csv_row() {
  local provider="$1"
  local storage_class="$2"
  local file_name="$3"
  local logical_path="$4"
  local run_index="$5"
  local bytes="$6"
  local elapsed_s="$7"
  local throughput="$8"

  echo "${provider},${storage_class},${file_name},${logical_path},${run_index},${bytes},${elapsed_s},${throughput}" >> "$RESULTS_CSV"
}

##############################################
## POBIERANIE Z AWS S3 (Standard)
##############################################

download_from_s3() {
  local s3_key="$1"
  local run_index="$2"

  local file_name
  file_name="$(basename "$s3_key")"
  local local_path="${DOWNLOAD_DIR}/aws-${s3_key//\//_}"

  echo
  echo "=== [AWS] Pobieranie obiektu: s3://${S3_BUCKET_STANDARD}/${s3_key} (class: ${S3_STORAGE_CLASS}, run ${run_index}) ==="

  local start_ns end_ns elapsed_s size_bytes throughput

  start_ns=$(now_ns)

  # UWAGA: brak --only-show-errors => widać progress + MB/s w AWS CLI
  aws s3 cp \
    "s3://${S3_BUCKET_STANDARD}/${s3_key}" \
    "$local_path"

  end_ns=$(now_ns)

  elapsed_s=$(elapsed_seconds "$start_ns" "$end_ns")
  size_bytes=$(stat -c%s "$local_path")
  throughput=$(calc_throughput_mb_s "$size_bytes" "$elapsed_s")

  echo "Czas pobrania  : ${elapsed_s} s"
  echo "Rozmiar        : ${size_bytes} B"
  echo "Średnie pasmo  : ${throughput} MB/s"

  append_csv_row "AWS" "$S3_STORAGE_CLASS" "$file_name" "$s3_key" "$run_index" "$size_bytes" "$elapsed_s" "$throughput"

  # Usunięcie pliku lokalnego, żeby nie zjadać miejsca na dysku
  rm -f "$local_path" || true
}

##############################################
## GŁÓWNY PRZEPŁYW
##############################################

main() {
  check_deps
  mkdir -p "$DOWNLOAD_DIR"
  write_csv_header

  echo "========================================="
  echo "Test pobierania plików z AWS S3 (class: $S3_STORAGE_CLASS)"
  echo "Wyniki w CSV (dopisywane): $RESULTS_CSV"
  echo "Katalog tymczasowy: $DOWNLOAD_DIR"
  echo "========================================="

  for key in "${S3_KEYS[@]}"; do
    for ((i=1; i<=RUNS_PER_FILE; i++)); do
      download_from_s3 "$key" "$i"
    done
  done

  echo
  echo "Zakończono test AWS."
}

main "$@"

#!/bin/bash

echo "🔄 Activating conda environment: blink371"
conda activate blink371

# Set the output path
BLINK_MODEL_NAME=$1
VERSION=v6
DATA_QUALITY=$4

echo "📦 Model name: $BLINK_MODEL_NAME"
echo "📊 Version: $VERSION"
echo "📈 Data quality: $DATA_QUALITY"

# Set paths based on text length
if [ "$2" = "kurz" ]; then
    ORIGINAL_DATA_PATH="../data/blink_data/$VERSION/$DATA_QUALITY/kurz"
    ENTITY_CATALOGUE="../Paulys_kb/blink_real_dict_v1_Kurz.jsonl"
    echo "✂️ Text length: kurz"
else
    ORIGINAL_DATA_PATH="../data/blink_data/$VERSION/$DATA_QUALITY/voll"
    ENTITY_CATALOGUE="../Paulys_kb/blink_real_dict_v1_Voll.jsonl"
    echo "📜 Text length: voll"
fi

# Determine mode and test mentions
if [ "$3" = "test" ]; then
    MODE="test"
    TEST_MENTIONS="$ORIGINAL_DATA_PATH/test.jsonl"
else
    MODE="output"
    TEST_MENTIONS=$3
fi

echo "🧪 Evaluation mode: $MODE"
echo "📄 Test mentions file: $TEST_MENTIONS"

# First run: with --keep_all True
echo "🚀 Running BLINK dense model with --keep_all True..."
python ../adjusted_blink_files/main_dense.py \
    --test_mentions "$TEST_MENTIONS" \
    --entity_catalogue "$ENTITY_CATALOGUE" \
    --top_k 64 \
    --crossencoder_model ../models/$BLINK_MODEL_NAME/crossencoder/pytorch_model.bin \
    --crossencoder_config ../models/$BLINK_MODEL_NAME/crossencoder/training_params.txt \
    --biencoder_model ../models/$BLINK_MODEL_NAME/biencoder/pytorch_model.bin \
    --biencoder_config ../models/$BLINK_MODEL_NAME/biencoder/training_params.txt \
    --output ../models/$BLINK_MODEL_NAME/logs \
    --entity_encoding ../models/$BLINK_MODEL_NAME/cand_encoding \
    --keep_all True

# echo "✅ First run complete (with keep_all)."

# Second run: without --keep_all
echo "🚀 Running BLINK dense model without --keep_all..."
python ../adjusted_blink_files/main_dense.py \
    --test_mentions "$TEST_MENTIONS" \
    --entity_catalogue "$ENTITY_CATALOGUE" \
    --top_k 64 \
    --crossencoder_model ../models/$BLINK_MODEL_NAME/crossencoder/pytorch_model.bin \
    --crossencoder_config ../models/$BLINK_MODEL_NAME/crossencoder/training_params.txt \
    --biencoder_model ../models/$BLINK_MODEL_NAME/biencoder/pytorch_model.bin \
    --biencoder_config ../models/$BLINK_MODEL_NAME/biencoder/training_params.txt \
    --output ../models/$BLINK_MODEL_NAME/logs \
    --entity_encoding ../models/$BLINK_MODEL_NAME/cand_encoding

echo "✅ Second run complete (without keep_all)."
echo "🎉 All evaluations finished!"
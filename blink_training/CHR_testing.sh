
#!/bin/bash

data_version=v6

for data_quality in gold gold_silver
do
    for text_length in voll kurz
    do
        echo "Running for: $data_quality $text_length"
        bash test_BLINK_model.sh models/$data_quality_$text_length $text_length test manual
        mkdir -p ../test_results/${text_length}_${data_quality}
        mv /models/${text_length}_${data_quality}/logs/* ../test_results/${text_length}_${data_quality}/
done

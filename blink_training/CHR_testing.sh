
#!/bin/bash

data_version=v6

for data_quality in full manual
do
    for text_length in kurz voll
    do
        echo "Running for: $data_quality $text_length"
        bash test_BLINK_model.sh blink_CHR_v2_${text_length}_${data_quality} $text_length test manual
        mkdir -p ../test_results/${text_length}_${data_quality}
        mv ../models/${text_length}_${data_quality}/logs/* ../test_results/${text_length}_${data_quality}/
    done
done

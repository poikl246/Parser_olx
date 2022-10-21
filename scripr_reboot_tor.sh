#!/bin/bash
for ((;;))
do
    COMMAND="service tor restart"
    ${COMMAND}

    echo "Перезапустил тор"

    COMMAND="service tor status"
    ${COMMAND}


    for ((i=300000; i > 0; i--))
    do
    echo $i
    sleep 1
    done

done


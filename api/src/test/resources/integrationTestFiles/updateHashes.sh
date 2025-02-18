#!/bin/bash
#set -x v

HASH_FILE="hashes.txt"
echo ${HASH_FILE}

pushd $(dirname "$0") > /dev/null

if [[ -f "${HASH_FILE}" ]] ; then
    while [[ "${OVERWRITE_FILE}" != 'yes' && "${OVERWRITE_FILE}" != 'no' ]] ; do 
        echo -n "${HASH_FILE} already exists.  Would you like to overwrite it? (yes/no): "
        read -r OVERWRITE_FILE
        echo
        if [[ ${OVERWRITE_FILE} == "no" ]] ; then
            echo !!! ABORT : not overwriting file
            exit 1
        elif [[ ${OVERWRITE_FILE} == "yes" ]] ; then
            echo Removing and recreating ${HASH_FILE}
            rm ${HASH_FILE}
        else 
            echo "Answer must be 'yes' or 'no'" 
        fi
    done
fi

touch ${HASH_FILE}

for file in $(ls integration_test_file_*) ; do
  HASH=$( sha256sum -b ${file} )
  echo ${HASH} >> ${HASH_FILE}
done

cat ${HASH_FILE}
popd > /dev/null

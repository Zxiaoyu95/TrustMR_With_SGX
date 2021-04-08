#!/bin/bash
docker exec -it slave1 /bin/bash ./runhosts.sh \& exit
docker exec -it slave2 /bin/bash ./runhosts.sh \& exit
docker exec -it slave3 /bin/bash ./runhosts.sh \& exit
docker exec -it slave4 /bin/bash ./runhosts.sh \& exit
docker exec -it slave5 /bin/bash ./runhosts.sh \& exit
#docker exec -it slave6 /bin/bash ./runhosts.sh \& exit
docker exec -it slave7 /bin/bash ./runhosts.sh \& exit
docker exec -it Master /bin/bash

# Créer un access token

### Pré-requis
- Créer un alias
```bash
mc alias set alias_name s3_endpoint access_key secret_key
```
Remplacer les valeurs alias_name, s3_endpoint, access_key, et secret_key

### Exécuter le script

Avant d'exécuter le script, il faut mettre à jour les différentes variables ainsi que la policy que vous souhaitez affecter.

- Définir les variables

```python
ALIAS_NAME = "alias_name"
BUCKET = "bucket"
KEY = "key/path/*"
```

La variable `ALIAS_NAME` doit nécessairement être celle créée avec la commande précédente.  
Pour la `KEY`, renseigner `*` pour un accès à tout le bucket.

- Définir la policy
```python
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET}/{KEY_ACCESS}"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:ListBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{BUCKET}"
                ],
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [
                            f"{KEY_ACCESS}/*"
                        ]
                    }
                }
            }
        ]
    }
```
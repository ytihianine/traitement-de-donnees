# Gérer le catalogue et les tables Icebergs

## Apache Polaris



## PyIceberg
### Supprimer une table du catalogue
Supprimer une table du catalogue ne supprime pas les données présentes sur S3

### Supprimer les données d'une table
Après avoir supprimé la table du catalogue, il est possible de supprimer les données présentes sur S3.

- Générer un access token MinIO

Se référer à ce [guide](../users/s3/readme.md) pour créer un access token et créer un alias MinIO.  
Pour voir la liste des alias
```bash
mc alias list
```

- Supprimer la key

```bash
mc rm --recursive --versions --force token_alias/bucket/path/to/your/key
```
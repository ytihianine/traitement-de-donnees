---
name: write-docs
description: "Rédiger la documentation projet. Utiliser pour : créer un document d'architecture, écrire un guide, rédiger un ADR, documenter une décision, ajouter de la documentation dans docs/. Mots-clés : documentation, docs, écrire, rédiger, documenter, guide, architecture, ADR."
argument-hint: "Décrivez le sujet à documenter et le type (architecture, guide, adr)"
---

# Rédaction de Documentation

## Quand utiliser

- Créer ou mettre à jour un document dans `docs/`
- Rédiger un document d'architecture, un guide ou un ADR
- Documenter une décision technique ou un flux applicatif

## Structure du dossier docs/

```
docs/
├── architecture/    # Documents d'architecture et de conception
├── guides/          # Guides pratiques et tutoriels
└── adr/             # Architecture Decision Records
```

## Convention de nommage

- Fichiers en **kebab-case** : `auth-flow.md`, `setup-dev-environment.md`
- Extension : `.md` (Markdown)
- Les ADR sont préfixés d'un numéro séquentiel : `001-choix-base-de-donnees.md`

## Langue

Toute la documentation est rédigée en **français**.

## Procédure

1. **Identifier la catégorie** : déterminer si le document est un `architecture`, un `guide` ou un `adr`.
2. **Nommer le fichier** en kebab-case et le placer dans le bon sous-dossier de `docs/`.
3. **Appliquer le template** correspondant à la catégorie (voir ci-dessous).
4. **Rédiger le contenu** en respectant les sections obligatoires du template.
5. **Vérifier** que tous les liens internes et références sont valides.

## Templates par catégorie

### Architecture (`docs/architecture/`)

Utiliser le template : [architecture.md](./assets/architecture.md)

Sections obligatoires :
- **Titre** : nom clair du composant ou du système documenté
- **Résumé** : description en 2-3 phrases de l'objet du document
- **Contexte** : pourquoi ce document existe, quel problème il adresse
- **Détails** : description technique approfondie avec diagrammes si nécessaire
- **Références** : liens vers les documents liés, ADR associés, ressources externes

### Guide (`docs/guides/`)

Utiliser le template : [guide.md](./assets/guide.md)

Sections obligatoires :
- **Titre** : action claire (ex. « Configurer l'environnement de développement »)
- **Résumé** : ce que le lecteur saura faire à la fin du guide
- **Contexte** : prérequis et public cible
- **Détails** : étapes numérotées avec exemples de code si nécessaire
- **Références** : liens vers la documentation officielle, guides connexes

### ADR (`docs/adr/`)

Utiliser le template : [adr.md](./assets/adr.md)

Sections obligatoires :
- **Titre** : `NNN - Intitulé de la décision` (ex. `001 - Choix de la base de données`)
- **Résumé** : la décision prise en une phrase
- **Contexte** : situation actuelle, contraintes, forces en présence
- **Détails** : options envisagées avec avantages/inconvénients, décision retenue et justification
- **Références** : ADR liés, documents d'architecture associés

## Règles de rédaction

- Utiliser un ton professionnel et concis
- Privilégier les listes à puces et les tableaux pour la clarté
- Inclure des exemples de code dans des blocs ` ``` ` avec le langage spécifié
- Les titres de sections utilisent le format Markdown (`##`, `###`)
- Pas de section changelog dans les documents

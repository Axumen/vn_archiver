# VN Archiver -- Domain Logic Explanation (Project Context)

## 📌 Purpose

This document explains **why domain logic is necessary** in the VN
Archiver project, using concrete examples from the system.

The goal is to move from:

file → hash → stored

To:

VN → Build → Artifact → File

------------------------------------------------------------------------

## 🧠 What is Domain Logic?

Domain logic represents the **real-world meaning and rules** of your
system.

In your case: - Visual Novels (VN) - Releases (Builds) - Distributions
(Artifacts) - Files (binary data)

------------------------------------------------------------------------

## 🔥 The Core Problem (Current System)

Your current ingestion logic:

SELECT id FROM versions WHERE sha256=?

This only answers:

"Have I seen this file before?"

But your real questions are:

-   Is this file part of an existing build?
-   Is this a patch or full game?
-   Is this an April Fools version?
-   Which VN does this belong to?

------------------------------------------------------------------------

## 🧱 Key Insight

Your project is NOT:

A collection of files

It is:

A structured archive of VN releases and relationships

------------------------------------------------------------------------

## 🧩 Domain Model (Required Structure)

### VN

vn_id\
title\
developer

### Build

build_id\
vn_id\
version_string\
release_type\
release_status

### Artifact

artifact_id\
build_id\
type\
platform

### File

file_id\
sha256\
filename\
size

------------------------------------------------------------------------

## ⚠️ Critical Distinction

Build ≠ File\
Artifact ≠ File\
File = raw data only

------------------------------------------------------------------------

## 🧪 Example

Input:

MyVN_v1.zip\
MyVN_v2_patch.zip\
MyVN_aprilfools.zip

With domain logic:

VN: MyVN

Builds: - v1 → full\
- v2 → patch → depends on v1\
- april_fools → parody

------------------------------------------------------------------------

## 🧠 Final Mental Model

Files → facts\
Artifacts → packaging\
Builds → meaning\
VN → identity

------------------------------------------------------------------------

## 📌 Conclusion

Without domain logic → flat hash database\
With domain logic → structured VN archive

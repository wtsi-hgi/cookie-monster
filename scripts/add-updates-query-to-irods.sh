#!/usr/bin/env bash
query=$(cat "resources/specific-queries/get-updates.sql")
iadmin asq "$query" updates
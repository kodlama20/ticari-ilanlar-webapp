#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Pydantic request models for the API."""

from typing import Optional
from pydantic import BaseModel

class ToolDateIn(BaseModel):
    text: str

class ToolCompanyIn(BaseModel):
    name: str

class ToolMudurlukIn(BaseModel):
    name: str

class ToolIlanTuruIn(BaseModel):
    term: str

class SearchFilters(BaseModel):
    date_from: Optional[str] = None
    date_to:   Optional[str] = None
    company_code: Optional[int] = None
    city_code:    Optional[int] = None
    type_code:    Optional[int] = None
    limit:        Optional[int] = None

class SearchIn(BaseModel):
    filters: 'SearchFilters'
    limit: Optional[int] = 40

class AnswerIn(BaseModel):
    filters: 'SearchFilters'
    q_tr: Optional[str] = None
    max_ctx: Optional[int] = 20

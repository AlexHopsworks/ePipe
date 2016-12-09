/*
 * Copyright (C) 2016 Hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

/* 
 * File:   Tables.h
 * Author: Mahmoud Ismail<maism@kth.se>
 *
 */

#ifndef TABLES_H
#define TABLES_H

//Dataset Table
static const char* DS = "dataset";
static const int NUM_DS_COLS = 7;
static const char* DS_COLS_TO_READ[] =    
   { "id",
     "inode_id",
     "inode_pid",
     "inode_name",
     "projectId",
     "description",
     "public_ds"
    };

static const int DS_PK = 0;
static const int DS_INODE_ID = 1;
static const int DS_INODE_PARENT_ID = 2;
static const int DS_INODE_NAME = 3;
static const int DS_PROJECT_ID = 4;
static const int DS_DESC = 5;
static const int DS_PUBLIC = 6;


//Project Table
static const char* PR = "project";
static const int NUM_PR_COLS = 5;
static const char* PR_COLS_TO_READ[] =    
   { "id",
     "inode_pid",
     "inode_name",
     "username",
     "description"
    };

static const int PR_PK= 0;
static const int PR_INODE_PID = 1;
static const int PR_INODE_NAME = 2;
static const int PR_USER = 3;
static const int PR_DESC = 4;


//INode->Dataset Lookup Table
static const char* INODE_DATASET_LOOKUP = "hdfs_inode_dataset_lookup";
static const int NUM_INODE_DATASET_COLS = 2;
static const char* INODE_DATASET_LOOKUP_COLS_TO_READ[] = {"inode_id", "dataset_id"};
static const int INODE_DATASET_LOOKUP_INODE_ID_COL = 0;
static const int INODE_DATASET_LOOKUO_DATASET_ID_COL = 1;


#endif /* TABLES_H */

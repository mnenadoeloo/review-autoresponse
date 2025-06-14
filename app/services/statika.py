"""Statika repository for accessing product information."""

from typing import Dict, Set, Any, Optional
import asyncpg
import json
import logging

import requests

import aiohttp
import asyncio
import re


logger = logging.getLogger(__name__)

class StatikaRepository():
    """Repository for interacting with Statika."""
    
    def __init__(self) -> None:
        """Initialize StatikaRepository repository."""
        super().__init__()
        max_concurrent_requests = 50
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.timeout = 10

    async def shard_name(self, nm_id: int) -> str:
        """Get shard number for a single ID.
        
        Args:
            nm_id: Product ID to fetch
            
        Returns:
            Basket name for Statika nm_id's url
        """

        basket = requests.get(f"...", headers={'Content-Type': 'application/json'}).json()['shard']
        pattern = r"-(\d+)\."

        try:
            return re.search(pattern, basket).group(1)
            
        except Exception as e:
            logger.error(f"Error fetching shard name: {str(e)}")
            return ""


    async def get_card_from_statika(self, nm_id, session) -> Optional[Dict[str, Any]]:
        """Fetch product information for one ID.

        Args:
            nm_id: Product ID to fetch

        Returns:
            Dictionary mapping product ID to its data
        """
        nm_id = int(nm_id)
        
        shard = await self.shard_name(nm_id=nm_id)
        
        if not shard:
            logger.error(f"Could not find the shard number in Statika.")
            return {}
            
        nm_url = f"..."
        headers = {
                'Content-Type': 'application/json',
            }
        
        params = {
                'limit': 1,
                'key': 0,
            }

        full_data = dict()
        
        try:
            async with session.get(nm_url, params=params, headers=headers, timeout=self.timeout) as response:
                if response.status == 200:
                    response_dict = await response.json()
                    if 'subj_root_name' in response_dict.keys():
                        full_data['parentname'] = response_dict['subj_root_name']

                    if 'description' in response_dict.keys():
                        full_data['Описание'] = response_dict['description']
                
                    if 'options' in response_dict.keys():
                        for inner_dict in response_dict['options']:
                            full_data[inner_dict['name']] = inner_dict['value']
                
                    if 'sizes_table' in response_dict.keys():
                        for each_size in response_dict['sizes_table']['values']:
                            full_data[f'Размер производителя {each_size["tech_size"]}'] = dict(zip(response_dict['sizes_table']['details_props'], each_size['details']))
            
                    if 'grouped_options' in response_dict.keys():
                        for grouped_option_dict in response_dict['grouped_options']:
                            for char in grouped_option_dict['options']:
                                full_data[char['name']] = char['value']
                    return full_data

                else:
                    return {"error": f"Failed to fetch from {nm_url}"}
                    
        except Exception as e:
            logger.error(f"Error fetching products info: {str(e)}")
            return {}
        
    async def fetch_products_info(self, nm_ids: Set[int]) -> Optional[Dict[str, Any]]:
        """Fetch product information for multiple IDs efficiently.

        Args:
            nm_ids: Set of product IDs to fetch

        Returns:
            Dictionary mapping product IDs to their data
        """
        async with aiohttp.ClientSession() as session:
            async def fetch_with_limit(nm_id):
                async with self.semaphore:
                    return await self.get_card_from_statika(nm_id, session=session)

            tasks = [fetch_with_limit(nm_id) for nm_id in nm_ids]
            results = await asyncio.gather(*tasks)

            full_results = {nm_id: results[idx] for idx, nm_id in enumerate(nm_ids)}
            
            return full_results